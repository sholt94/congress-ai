#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BILLSTATUS XML → Postgres ETL (GovInfo + older variants, BULK optimized w/ cosponsor dedupe)
- Recursively finds BILLSTATUS-*.xml and fdsys_billstatus.xml
- Extracts identity from XML (<type>/<number>/<congress>), filename, or dirs
- Upserts 'bills' per bill
- BULK INSERTS 'bill_actions' and 'bill_cosponsors' with psycopg2.extras.execute_values
- Deduplicates cosponsors within each flush to avoid ON CONFLICT double-hit
- Commits in batches for speed
- Optional --limit N to process only first N files (for a small test)

pip install psycopg2-binary python-dateutil tqdm
"""

import os, re, glob, sys, argparse
from datetime import datetime
from dateutil import parser as dtp
import xml.etree.ElementTree as ET
import psycopg2
import psycopg2.extras as extras
from tqdm import tqdm

# ---------- config ----------
ROOT = "/Users/skylerholt/civic/congress"
DATA_PATTERNS = [
    os.path.join(ROOT, "data", "**", "BILLSTATUS-*.xml"),
    os.path.join(ROOT, "data", "**", "fdsys_billstatus.xml"),
]

COMMIT_EVERY_BILLS = 5000   # commit every N bills
BULK_PAGE_SIZE     = 1000   # page size for execute_values
BUFFER_FLUSH_ROWS  = 20000  # flush action/cosponsor buffers when total rows exceed this

# ---------- utils ----------
def load_env():
    env_path = os.path.join(ROOT, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line=line.strip()
                if not line or line.startswith("#") or "=" not in line: continue
                k,v=line.split("=",1)
                os.environ.setdefault(k.strip(), v.strip())

def strip_ns(tree):
    for el in tree.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}",1)[1]
    return tree

def safe_date(s):
    try: return dtp.parse(s).date()
    except: return None

def safe_dt(s):
    try: return dtp.parse(s)
    except: return None

# ---------- identity extraction ----------
def parse_identity_from_xml(root):
    congress=bill_type=bill_number=None
    for el in root.iter():
        tn = el.tag.lower()
        tx = (el.text or "").strip()
        if not tx: continue
        if congress is None and (tn == "congress" or tn.endswith("congress")):
            dd = re.sub(r"\D","",tx); 
            if dd: congress = int(dd)
        if bill_type is None and (tn in ("type","billtype") or tn.endswith("billtype")):
            bill_type = tx.lower()
        if bill_number is None and (tn in ("number","billnumber") or tn.endswith("billnumber")):
            dd = re.sub(r"\D","",tx)
            if dd: bill_number = int(dd)
        if congress and bill_type and bill_number:
            break
    return congress, bill_type, bill_number

def parse_identity_from_filename(path):
    m = re.search(r"BILLSTATUS-(\d+)([a-z]+)(\d+)\.xml$", os.path.basename(path), re.I)
    if m:
        return int(m.group(1)), m.group(2).lower(), int(m.group(3))
    return None, None, None

def parse_identity_from_dirs(path):
    parts = path.split(os.sep)
    idxs = [i for i,p in enumerate(parts) if p.lower() in ("bills","billstatus")]
    if not idxs: return None, None, None
    for idx in reversed(idxs):
        try:
            congress = int(parts[idx-1])
        except Exception:
            continue
        bill_type = parts[idx+1].lower()
        bill_id = parts[idx+2].lower() if len(parts) > idx+2 else ""
        m = re.match(r"([a-z]+)(\d+)$", bill_id)
        if m:
            return congress, bill_type, int(m.group(2))
    return None, None, None

# ---------- other field parsing ----------
def text_candidates(root, names):
    vals=set()
    for el in root.iter():
        tn = el.tag.lower()
        if tn in names or any(tn.endswith(n) for n in names):
            tx=(el.text or "").strip()
            if tx: vals.add(tx)
    return list(vals)

def parse_one(xml_path):
    try:
        tree = ET.parse(xml_path)
    except Exception as e:
        return None, f"parse error: {e}"
    root = strip_ns(tree).getroot()

    c,t,n = parse_identity_from_xml(root)
    if any(x is None for x in (c,t,n)):
        c2,t2,n2 = parse_identity_from_filename(xml_path)
        c = c or c2; t = t or t2; n = n or n2
    if any(x is None for x in (c,t,n)):
        c3,t3,n3 = parse_identity_from_dirs(xml_path)
        c = c or c3; t = t or t3; n = n or n3
    if any(x is None for x in (c,t,n)):
        return None, "missing identity"

    t = t.lower()

    titles = text_candidates(root, {"title","officialtitle","titlewithoutnumber"})
    title = sorted(titles, key=len)[0] if titles else None

    introduced_date=None
    for el in root.iter():
        if el.tag.lower().endswith("introduceddate") and el.text:
            introduced_date = safe_date(el.text); break

    sponsor_bioguide = sponsor_fullname = None
    for el in root.iter():
        if el.tag.lower().endswith("sponsor") or el.tag.lower().endswith("sponsors"):
            for kid in el.iter():
                kn=kid.tag.lower(); tx=(kid.text or "").strip()
                if kn.endswith("bioguideid") and tx: sponsor_bioguide=tx
                elif (kn.endswith("fullname") or kn.endswith("name")) and tx and not sponsor_fullname:
                    sponsor_fullname=tx
            if sponsor_bioguide or sponsor_fullname: break

    cosponsors=[]
    for cs in root.iter():
        if cs.tag.lower().endswith("cosponsors"):
            for csp in cs:
                if not isinstance(csp.tag,str): continue
                if not (csp.tag.lower().endswith("cosponsor") or csp.tag.lower()=="item"): continue
                bio=fulln=party=state=None; joined=None; is_orig=None
                for kid in csp.iter():
                    kn=kid.tag.lower(); tx=(kid.text or "").strip()
                    if kn.endswith("bioguideid") and tx: bio=tx
                    elif kn.endswith("fullname") and tx: fulln=tx
                    elif kn.endswith("party") and tx: party=tx
                    elif kn.endswith("state") and tx: state=tx
                    elif kn.endswith("sponsorshipdate") and tx: joined=safe_date(tx)
                    elif kn.endswith("isoriginalcosponsor") and tx: is_orig=tx.lower() in ("true","1","yes")
                if bio: cosponsors.append((bio,fulln,party,state,joined,is_orig))
            break

    actions=[]
    for acts in root.iter():
        if acts.tag.lower().endswith("actions"):
            for a in acts:
                if not isinstance(a.tag,str): continue
                if not (a.tag.lower().endswith("action") or a.tag.lower()=="item"): continue
                dt=txt=actor=code=None
                for kid in a:
                    kn=kid.tag.lower(); tx=(kid.text or "").strip()
                    if kn.endswith("actiondatetime") or kn.endswith("actiondate"): dt=safe_dt(tx)
                    elif kn.endswith("text") and tx: txt=tx
                    elif kn.endswith("actors") and tx: actor=tx
                    elif kn.endswith("actioncode") and tx: code=tx
                if dt or txt: actions.append((dt,actor,txt,code))
            break

    latest_action_date = latest_action_text = None
    if actions:
        latest = sorted(actions, key=lambda x: (x[0] or datetime.min))[-1]
        latest_action_date, latest_action_text = latest[0], latest[2]

    chamber = "house" if t.startswith("h") else ("senate" if t.startswith("s") else None)

    rec = {
        "congress": c, "bill_type": t, "bill_number": n,
        "chamber": chamber, "title": title, "introduced_date": introduced_date,
        "latest_action": latest_action_text, "latest_action_date": latest_action_date,
        "sponsor_bioguide": sponsor_bioguide, "sponsor_fullname": sponsor_fullname,
        "actions": actions, "cosponsors": cosponsors,
        "source_path": os.path.relpath(xml_path, ROOT),
    }
    return rec, None

# ---------- bulk helpers ----------
def _dedupe_cosponsors(rows):
    merged = {}
    for (c,t,n,bio,fulln,party,state,joined,is_orig) in rows:
        key = (c,t,n,bio)
        if key not in merged:
            merged[key] = [c,t,n,bio,fulln,party,state,joined,is_orig]
        else:
            cur = merged[key]
            cur[4] = fulln if fulln else cur[4]
            cur[5] = party if party else cur[5]
            cur[6] = state if state else cur[6]
            cur[7] = joined if joined else cur[7]
            cur[8] = True if (is_orig is True or cur[8] is True) else (is_orig if is_orig is not None else cur[8])
    return [tuple(v) for v in merged.values()]

def flush_buffers(cur, actions_buf, cosponsors_buf):
    if actions_buf:
        extras.execute_values(
            cur,
            """
            insert into bill_actions
              (congress,bill_type,bill_number,action_datetime,actor,action_text,action_code,source_path)
            values %s
            on conflict (congress,bill_type,bill_number,action_datetime,action_text) do nothing
            """,
            actions_buf,
            page_size=BULK_PAGE_SIZE
        )
        actions_buf.clear()

    if cosponsors_buf:
        deduped = _dedupe_cosponsors(cosponsors_buf)
        if deduped:
            extras.execute_values(
                cur,
                """
                insert into bill_cosponsors
                  (congress,bill_type,bill_number,bioguide,fullname,party,state,joined_date,is_original)
                values %s
                on conflict (congress,bill_type,bill_number,bioguide) do update set
                  fullname    = coalesce(excluded.fullname, bill_cosponsors.fullname),
                  party       = coalesce(excluded.party, bill_cosponsors.party),
                  state       = coalesce(excluded.state, bill_cosponsors.state),
                  joined_date = coalesce(excluded.joined_date, bill_cosponsors.joined_date),
                  is_original = coalesce(excluded.is_original, bill_cosponsors.is_original)
                """,
                deduped,
                page_size=BULK_PAGE_SIZE
            )
        cosponsors_buf.clear()

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Parse only; no DB writes.")
    ap.add_argument("--limit", type=int, default=0, help="Process only first N files (0 = all).")
    args = ap.parse_args()

    load_env()
    dburl = os.environ.get("DATABASE_URL")

    files=[]
    for pat in DATA_PATTERNS:
        files.extend(glob.glob(pat, recursive=True))
    files = sorted(set(files))
    if args.limit and len(files) > args.limit:
        files = files[:args.limit]
    if not files:
        print("No BILLSTATUS XMLs found under", os.path.join(ROOT,"data"))
        sys.exit(1)

    if args.dry_run:
        ok=0; bad=0
        for p in tqdm(files, desc="DRY-RUN parse"):
            rec, err = parse_one(p)
            if rec: ok+=1
            else: bad+=1
        print(f"\nDRY-RUN done. Would insert OK={ok}, Skipped={bad}")
        return

    if not dburl:
        print("ERROR: DATABASE_URL not set in .env")
        sys.exit(1)

    try:
        conn = psycopg2.connect(dburl)
    except Exception as e:
        print("DB connection failed:", e); sys.exit(1)
    conn.autocommit = False
    cur = conn.cursor()

    ok=0; bad=0; batch_bills=0
    actions_buf=[]
    cosponsors_buf=[]

    for p in tqdm(files, desc="BILLSTATUS → DB (batched)"):
        rec, err = parse_one(p)
        if not rec: bad+=1; continue

        cur.execute("""
            insert into bills
              (congress,bill_type,bill_number,chamber,title,introduced_date,latest_action,latest_action_date,
               sponsor_bioguide,sponsor_fullname)
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (congress,bill_type,bill_number) do update set
              chamber             = excluded.chamber,
              title               = coalesce(excluded.title, bills.title),
              introduced_date     = coalesce(excluded.introduced_date, bills.introduced_date),
              latest_action       = coalesce(excluded.latest_action, bills.latest_action),
              latest_action_date  = coalesce(excluded.latest_action_date, bills.latest_action_date),
              sponsor_bioguide    = coalesce(excluded.sponsor_bioguide, bills.sponsor_bioguide),
              sponsor_fullname    = coalesce(excluded.sponsor_fullname, bills.sponsor_fullname);
        """, (
            rec["congress"], rec["bill_type"], rec["bill_number"], rec["chamber"],
            rec["title"], rec["introduced_date"], rec["latest_action"], rec["latest_action_date"],
            rec["sponsor_bioguide"], rec["sponsor_fullname"]
        ))

        for (dt, actor, txt, code) in rec["actions"]:
            actions_buf.append((rec["congress"], rec["bill_type"], rec["bill_number"], dt, actor, txt, code, rec["source_path"]))
        for (bio,fulln,party,state,joined,is_orig) in rec["cosponsors"]:
            cosponsors_buf.append((rec["congress"], rec["bill_type"], rec["bill_number"], bio, fulln, party, state, joined, is_orig))

        ok+=1
        batch_bills+=1

        if len(actions_buf) + len(cosponsors_buf) >= BUFFER_FLUSH_ROWS:
            flush_buffers(cur, actions_buf, cosponsors_buf)

        if batch_bills >= COMMIT_EVERY_BILLS:
            flush_buffers(cur, actions_buf, cosponsors_buf)
            conn.commit()
            batch_bills = 0

    flush_buffers(cur, actions_buf, cosponsors_buf)
    conn.commit()
    conn.close()
    print(f"\nIngest complete. OK={ok}  Skipped={bad}")

if __name__ == "__main__":
    main()
