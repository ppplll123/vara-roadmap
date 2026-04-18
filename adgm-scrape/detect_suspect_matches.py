#!/usr/bin/env python3
"""
detect_suspect_matches.py
-------------------------
Local, zero-API-call suspect detector for ADGM/VARA LinkedIn enrichment matches.

Scores each firm 0-100 on likelihood of being a wrong LinkedIn match based on
five signals computed purely from local JSON data.

Signals + weights:
  1. Location mismatch  (40)
  2. Follower count     (25)
  3. Employee anomaly   (15)
  4. Industry mismatch  (10)
  5. Name similarity    (10)

Inputs:
  - vara_roadmap/adgm-scrape/firms-linkedin-enriched.json   (382 ADGM firms, Apify enriched)
  - vara_roadmap/entities-linkedin-enriched.json            (62  VARA entities, Apify enriched)
  - vara_roadmap/adgm-scrape/full-register-all-tabs.json    (ADGM register w/ address + approved_persons + activities)
  - vara_roadmap/adgm-scrape/scrappy-employees-flat.json    (employee profiles -> company URL cross-check, bonus strategy D)
  - antweave_roadmap/master-data.json                       (unified firms, including VARA + ADGM src)

Outputs (in vara_roadmap/adgm-scrape/):
  - linkedin-suspects.json          full per-firm scoring
  - linkedin-suspects-top100.md     human-readable top 100 table
  - linkedin-verified-ok.json       firms with suspect score < 20
  - linkedin-reconciliation-top20.md brainstorm of per-firm fix strategy
"""
import json
import os
import re
from collections import defaultdict, Counter
from pathlib import Path

# ---------- paths ----------
# Repo root = two parents up from this file (.../vara_roadmap/adgm-scrape/detect_suspect_matches.py)
ROOT = Path(__file__).resolve().parents[2]
ADGM_ENRICH = ROOT / "vara_roadmap/adgm-scrape/firms-linkedin-enriched.json"
VARA_ENRICH = ROOT / "vara_roadmap/entities-linkedin-enriched.json"
FULL_REG    = ROOT / "vara_roadmap/adgm-scrape/full-register-all-tabs.json"
EMP_FLAT    = ROOT / "vara_roadmap/adgm-scrape/scrappy-employees-flat.json"
MASTER      = ROOT / "antweave_roadmap/master-data.json"

OUT_DIR     = ROOT / "vara_roadmap/adgm-scrape"
OUT_SUSP    = OUT_DIR / "linkedin-suspects.json"
OUT_TOP100  = OUT_DIR / "linkedin-suspects-top100.md"
OUT_OK      = OUT_DIR / "linkedin-verified-ok.json"
OUT_RECON   = OUT_DIR / "linkedin-reconciliation-top20.md"

# ---------- heuristics config ----------
PLAUSIBLE_GLOBAL_HQ_COUNTRIES = {
    # ISO2 or common country strings we allow as "global parent" HQ for a UAE-licensed arm
    "uk", "united kingdom", "gb", "england", "scotland",
    "us", "usa", "united states", "united states of america",
    "ch", "switzerland",
    "sg", "singapore",
    "hk", "hong kong",
    "jp", "japan",
    "de", "germany",
    "fr", "france",
    "in", "india",
    "ca", "canada",    # frequent global parent
    "au",              # dicey but AU parent CAN exist for VASP; we DON'T allow -- override below
    "ie", "ireland",
    "lu", "luxembourg",
    "nl", "netherlands",
    "it", "italy",
    "es", "spain",
    "se", "sweden",
    "no", "norway",
    "dk", "denmark",
    "fi", "finland",
    "be", "belgium",
    "at", "austria",
    "il", "israel",
    "kr", "south korea", "korea",
    "cn", "china",
}

# Explicit whitelist per the task description (task-listed "allowed" global HQs)
TASK_ALLOWED = {"uk","us","ch","sg","hk","jp","de","fr","in",
                "united kingdom","united states","switzerland","singapore",
                "hong kong","japan","germany","france","india"}

# UAE indicator strings
UAE_HINTS = {"uae","united arab emirates","ae","dubai","abu dhabi","ajman","sharjah",
             "ras al khaimah","fujairah","umm al quwain","adgm","difc","difc - dubai"}

# Country strings that are DEFINITELY suspicious (Australia per user example, plus other
# uncommon global parents for a UAE VASP)
HARD_SUSPECT_COUNTRIES = {
    "au", "australia",
    "br", "brazil",
    "ar", "argentina",
    "ng", "nigeria",
    "ke", "kenya",
    "za", "south africa",
    "ph", "philippines",
    "vn", "vietnam",
    "ro", "romania",
    "pl", "poland",        # not impossible, but very unusual
    "cz", "czech republic",
    "ua", "ukraine",
    "ru", "russia",
    "tr", "turkey",
    "mx", "mexico",
    "pe", "peru",
    "cl", "chile",
    "id", "indonesia",
    "th", "thailand",
    "my", "malaysia",
    "pt", "portugal",
    "gr", "greece",
    "bg", "bulgaria",
    "rs", "serbia",
    "hu", "hungary",
}

# Industry buckets
BAD_INDUSTRIES = [
    "it services", "information technology & services",
    "computer", "electronics", "consumer services", "consumer goods",
    "marketing", "advertising", "public relations", "design",
    "retail", "apparel", "fashion", "food", "beverages",
    "hospitality", "restaurants", "hotels", "travel",
    "education", "e-learning", "entertainment", "media production",
    "animation", "broadcast media", "publishing",
    "construction", "real estate management", "automotive",
    "photography",
]

FINANCE_INDUSTRIES = [
    "financial services", "banking", "investment", "venture capital",
    "capital markets", "asset management", "insurance", "fund",
    "private equity", "investment banking", "financial"
]

# VASP/regulated-activity keywords (from ADGM activities)
REGULATED_ACTIVITY_KEYWORDS = [
    "managing assets", "managing a collective investment fund",
    "dealing in investments", "banking", "insurance", "advising on investments",
    "custody", "arranging deals in investments", "managing investments",
    "accepting deposits", "providing money services",
    "operating an exchange", "providing custody",
]

# ---------- helpers ----------
def load(p: Path):
    with p.open() as f:
        return json.load(f)

def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    # strip punctuation (keep alphanumerics & spaces)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    # common corporate suffix/qualifier tokens to drop
    drop = {
        "limited","ltd","llc","fze","dmcc","plc","inc","incorporated","corp","corporation",
        "mena","me","middle","east","capital","gulf","uae","holdings","holding",
        "group","co","company","sa","ag","gmbh","bv","nv","s","a","the","international",
        "investments","investment","partners","ventures","private","public",
    }
    tokens = [t for t in s.split() if t and t not in drop]
    return " ".join(tokens).strip()

def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    # DP
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            cur[j] = min(cur[j-1] + 1, prev[j] + 1, prev[j-1] + cost)
        prev = cur
    return prev[-1]

def name_similarity(a: str, b: str) -> float:
    """Return similarity [0..1], 1 = identical after normalization."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    dist = levenshtein(na, nb)
    return 1.0 - dist / max(len(na), len(nb))

def extract_country(hq: str) -> str:
    if not hq:
        return ""
    # "Zurich, CH" or "Conshohocken, PA, United States" or "Kilburn, AU"
    parts = [p.strip() for p in hq.split(",") if p.strip()]
    if not parts:
        return ""
    return parts[-1].lower()

def is_uae(hq_country: str, hq_full: str) -> bool:
    c = (hq_country or "").lower()
    full = (hq_full or "").lower()
    if c in UAE_HINTS:
        return True
    for hint in UAE_HINTS:
        if hint in full:
            return True
    return False

def industry_mismatch(industry: str, activities: list) -> bool:
    if not industry:
        return False
    ind_lc = industry.lower()
    # only flag mismatch when firm clearly does financial regulated work AND industry is non-finance-consumer
    acts_lc = " ".join([(a.get("type") or "").lower() if isinstance(a, dict) else "" for a in (activities or [])])
    firm_is_regulated_finance = any(kw in acts_lc for kw in REGULATED_ACTIVITY_KEYWORDS)
    looks_finance = any(fi in ind_lc for fi in FINANCE_INDUSTRIES)
    looks_non_finance = any(bi in ind_lc for bi in BAD_INDUSTRIES)
    if firm_is_regulated_finance and looks_non_finance and not looks_finance:
        return True
    return False

# ---------- load data ----------
adgm = load(ADGM_ENRICH)       # 382
vara = load(VARA_ENRICH)       # 62
register = load(FULL_REG)      # ADGM register (453)
employees = load(EMP_FLAT)     # scrappy employees
master = load(MASTER)          # unified

# index register by normalized company name
reg_idx = {}
for r in register:
    name = r.get("company") or ""
    reg_idx[normalize_name(name)] = r

# index scrappy employees by source_firm (raw name)
emp_by_firm = defaultdict(list)
for e in employees:
    sf = e.get("source_firm") or ""
    if sf:
        emp_by_firm[normalize_name(sf)].append(e)

# master firms by id+name
master_firms = master.get("firms", [])
master_by_name = {normalize_name(f.get("n","")): f for f in master_firms}

# ---------- score one firm ----------
def score_firm(firm_name: str, linkedin_url: str, linkedin_name: str,
               followers, employees_ct, hq: str, industry: str, website: str,
               source: str, register_entry: dict | None):
    signals = []
    score = 0.0

    # Identify register-side info
    addr = (register_entry or {}).get("address", "") if register_entry else ""
    activities = []
    if register_entry:
        a = register_entry.get("activities") or {}
        if isinstance(a, dict):
            activities = a.get("regulatedActivityVM") or []
    # ADGM register firms are UAE-based; VARA entities are UAE-based too.
    # For ADGM firms we still require an explicit UAE marker in the register
    # address — the previous `... or True` tautology silently masked any
    # location-mismatch signal for ADGM rows.
    addr_lower = addr.lower() if addr else ""
    firm_is_uae = (
        source == "VARA"
        or "united arab emirates" in addr_lower
        or "abu dhabi" in addr_lower
        or "dubai" in addr_lower
    )

    # --- signal 1: location mismatch (40) ---
    hq_country = extract_country(hq or "")
    loc_flag = False
    loc_reason = ""
    if hq:
        if is_uae(hq_country, hq):
            loc_flag = False
            loc_reason = "linkedin HQ in UAE ok"
        elif hq_country in HARD_SUSPECT_COUNTRIES:
            loc_flag = True
            loc_reason = f"hard-suspect country: {hq_country}"
            score += 40
        elif hq_country in TASK_ALLOWED:
            loc_reason = f"plausible global parent: {hq_country}"
        elif hq_country in PLAUSIBLE_GLOBAL_HQ_COUNTRIES:
            # weaker whitelist — half-weight
            loc_reason = f"secondary-whitelist country: {hq_country}"
        elif hq_country == "":
            loc_reason = "no country in hq"
        else:
            loc_flag = True
            loc_reason = f"unknown / non-whitelisted: {hq_country}"
            score += 25  # softer for unrecognized
    else:
        loc_reason = "no hq"
    signals.append({"name":"location","weight":40,"flagged":loc_flag,"reason":loc_reason,"hq":hq,"hq_country":hq_country})

    # --- signal 2: follower count (25) ---
    fol_flag = False
    fol_pts = 0
    fol_reason = ""
    f = followers or 0
    # Known mega brands boosters (if firm name contains these, expect huge followers)
    mega_tokens = ["binance","jpmorgan","j.p. morgan","jp morgan","morgan stanley","goldman",
                   "mckinsey","bcg","deloitte","pwc","kpmg","ernst","citi","hsbc","barclays",
                   "ubs","credit suisse","ubs","blackrock","bain","accenture","bank of america"]
    fn_lc = (firm_name or "").lower()
    is_mega = any(mt in fn_lc for mt in mega_tokens)
    if is_mega and f < 10000:
        fol_pts = 25
        fol_flag = True
        fol_reason = f"firm name implies mega brand but only {f} followers"
    elif f > 0 and f < 200:
        fol_pts = 25
        fol_flag = True
        fol_reason = f"regulated firm w/ very low followers ({f})"
    elif f > 0 and f < 1000:
        fol_pts = 10
        fol_flag = True
        fol_reason = f"regulated firm w/ low followers ({f})"
    elif f == 0:
        fol_pts = 5
        fol_reason = "no followers recorded"
    else:
        fol_reason = f"{f} followers ok"
    score += fol_pts
    signals.append({"name":"followers","weight":25,"flagged":fol_flag,"points":fol_pts,
                    "reason":fol_reason,"followers":f})

    # --- signal 3: employee anomaly (15) ---
    emp_pts = 0
    emp_flag = False
    emp_reason = ""
    # approved persons count from register
    ap_count = 0
    if register_entry:
        ap = register_entry.get("approved_persons") or {}
        if isinstance(ap, dict):
            items = ap.get("approvedPersonVM") or []
            ap_count = sum(1 for p in items if (p.get("status") or "").lower() == "active")
    lk_emp = employees_ct or 0
    if ap_count >= 10 and 0 < lk_emp <= 10:
        emp_pts = 15
        emp_flag = True
        emp_reason = f"ADGM has {ap_count} active approved persons but LinkedIn shows {lk_emp} employees"
    elif ap_count >= 5 and 0 < lk_emp <= 5:
        emp_pts = 10
        emp_flag = True
        emp_reason = f"ADGM has {ap_count} active approved persons but LinkedIn shows only {lk_emp} employees"
    elif lk_emp == 1 and ap_count >= 2:
        emp_pts = 8
        emp_flag = True
        emp_reason = f"Linkedin shows 1 employee, register has {ap_count} approved persons"
    else:
        emp_reason = f"LK {lk_emp} employees vs register {ap_count} approved persons"
    score += emp_pts
    signals.append({"name":"employees","weight":15,"flagged":emp_flag,"points":emp_pts,
                    "reason":emp_reason,"linkedin_employees":lk_emp,"active_approved_persons":ap_count})

    # --- signal 4: industry mismatch (10) ---
    ind_flag = industry_mismatch(industry or "", activities)
    ind_pts = 10 if ind_flag else 0
    ind_reason = f"industry='{industry}' activities[{len(activities)}]"
    score += ind_pts
    signals.append({"name":"industry","weight":10,"flagged":ind_flag,"points":ind_pts,
                    "reason":ind_reason,"industry":industry})

    # --- signal 5: name similarity (10) ---
    sim = name_similarity(firm_name or "", linkedin_name or "")
    # Threshold: <0.6 similarity → suspect (task said >40% characters differ => <60% sim).
    name_flag = sim < 0.6
    name_pts = 0
    if sim < 0.4:
        name_pts = 10
    elif sim < 0.6:
        name_pts = 6
    elif sim < 0.75:
        name_pts = 2
    score += name_pts
    signals.append({"name":"name_similarity","weight":10,"flagged":name_flag,"points":name_pts,
                    "reason":f"sim={sim:.2f} firm_norm='{normalize_name(firm_name or '')}' lk_norm='{normalize_name(linkedin_name or '')}'",
                    "similarity":round(sim,3)})

    return round(min(score, 100), 1), signals

# ---------- build unified record list ----------
records = []

# ADGM
for r in adgm:
    if not r.get("found"):
        continue
    firm_name = r.get("firm_name") or ""
    reg = reg_idx.get(normalize_name(firm_name))
    score, signals = score_firm(
        firm_name=firm_name,
        linkedin_url=r.get("linkedin_url"),
        linkedin_name=r.get("linkedin_name"),
        followers=r.get("followers"),
        employees_ct=r.get("employees"),
        hq=r.get("hq"),
        industry=r.get("industry"),
        website=r.get("website"),
        source="ADGM",
        register_entry=reg,
    )
    records.append({
        "source": "ADGM",
        "firm_name": firm_name,
        "suspect_score": score,
        "linkedin_url": r.get("linkedin_url"),
        "linkedin_name": r.get("linkedin_name"),
        "followers": r.get("followers"),
        "employees": r.get("employees"),
        "hq": r.get("hq"),
        "industry": r.get("industry"),
        "website": r.get("website"),
        "match_confidence": r.get("match_confidence"),
        "matched_via": r.get("matched_via"),
        "matched_query": r.get("matched_query"),
        "adgm_address": (reg or {}).get("address"),
        "adgm_fsp_number": (reg or {}).get("fsp_number"),
        "adgm_activities_count": len(((reg or {}).get("activities") or {}).get("regulatedActivityVM") or []),
        "adgm_approved_persons_count": len(((reg or {}).get("approved_persons") or {}).get("approvedPersonVM") or []),
        "signals": signals,
    })

# VARA
for r in vara:
    if not r.get("found"):
        continue
    firm_name = r.get("entity") or ""
    score, signals = score_firm(
        firm_name=firm_name,
        linkedin_url=r.get("linkedin_url"),
        linkedin_name=r.get("linkedin_name"),
        followers=r.get("followers"),
        employees_ct=r.get("employees"),
        hq=r.get("hq"),
        industry=r.get("industry"),
        website=r.get("website"),
        source="VARA",
        register_entry=None,
    )
    records.append({
        "source": "VARA",
        "firm_name": firm_name,
        "suspect_score": score,
        "linkedin_url": r.get("linkedin_url"),
        "linkedin_name": r.get("linkedin_name"),
        "followers": r.get("followers"),
        "employees": r.get("employees"),
        "hq": r.get("hq"),
        "industry": r.get("industry"),
        "website": r.get("website"),
        "match_score_apify": r.get("match_score"),
        "vara_status": r.get("status"),
        "signals": signals,
    })

# ---------- sort + partition ----------
records.sort(key=lambda x: x["suspect_score"], reverse=True)

high   = [r for r in records if r["suspect_score"] >= 60]
medium = [r for r in records if 40 <= r["suspect_score"] < 60]
low    = [r for r in records if 20 <= r["suspect_score"] < 40]
ok     = [r for r in records if r["suspect_score"] < 20]

summary = {
    "total_scored": len(records),
    "by_tier": {"high_gt60": len(high), "medium_40_60": len(medium), "low_20_40": len(low), "trusted_lt20": len(ok)},
    "top10": records[:10],
    "records": records,
}

# ---------- write outputs ----------
with OUT_SUSP.open("w") as f:
    json.dump(summary, f, indent=2)

with OUT_OK.open("w") as f:
    json.dump(ok, f, indent=2)

def fmt_md_row(i, r):
    firm = (r["firm_name"] or "")[:55]
    lk = r.get("linkedin_url") or "n/a"
    lk_name = (r.get("linkedin_name") or "")[:30]
    hq = (r.get("hq") or "")[:30]
    fol = r.get("followers") or 0
    emp = r.get("employees") or 0
    ind = (r.get("industry") or "")[:25]
    return f"| {i} | {r['suspect_score']:.0f} | {r['source']} | {firm} | {lk_name} | {fol} | {emp} | {hq} | {ind} | [link]({lk}) |"

top100 = records[:100]
with OUT_TOP100.open("w") as f:
    f.write("# Top 100 Suspect LinkedIn Matches\n\n")
    f.write(f"Total firms scored: **{len(records)}**  \n")
    f.write(f"High (>60): **{len(high)}** | Medium (40-60): **{len(medium)}** | Low (20-40): **{len(low)}** | Trusted (<20): **{len(ok)}**\n\n")
    f.write("| # | Score | Src | Firm | LK Name | Followers | Emp | HQ | Industry | URL |\n")
    f.write("|---|------|-----|------|---------|-----------|-----|----|----------|-----|\n")
    for i, r in enumerate(top100, 1):
        f.write(fmt_md_row(i, r) + "\n")

    f.write("\n\n## Signal breakdown for top 20\n\n")
    for i, r in enumerate(records[:20], 1):
        f.write(f"### {i}. {r['firm_name']} — score {r['suspect_score']} ({r['source']})\n")
        f.write(f"- LinkedIn: {r.get('linkedin_url')}  \n")
        f.write(f"- LK name: {r.get('linkedin_name')}  \n")
        f.write(f"- HQ: {r.get('hq')}  \n")
        f.write(f"- Followers: {r.get('followers')}, Employees: {r.get('employees')}, Industry: {r.get('industry')}\n")
        for s in r["signals"]:
            mark = "⚠" if s.get("flagged") else "·"
            pts = s.get("points", 0)
            f.write(f"  - {mark} {s['name']} (w{s['weight']}, +{pts}): {s['reason']}\n")
        f.write("\n")

# ---------- Bonus: reconciliation strategies for top 20 ----------
def suggest_strategy(r):
    """Return list of strategy labels (A/B/C/D) with per-firm notes."""
    strategies = []
    firm = r["firm_name"]
    lk_url = r.get("linkedin_url") or ""
    norm = normalize_name(firm)
    dashed = "-".join(norm.split())
    # Strategy A: dashed-name slug guess
    if dashed and dashed not in (lk_url or "").lower():
        strategies.append(("A", f"try URL slug `linkedin.com/company/{dashed}/` (canonical dashed form of '{firm}')"))
    # Strategy B: firm's own website
    if r.get("website") and r["website"] not in ("", None):
        strategies.append(("B", f"scrape the firm's website ({r['website']}) for an <a href='linkedin.com/...'> link — MOST RELIABLE (once current website is validated)"))
    else:
        strategies.append(("B", "no website on record — skip"))
    # Strategy C: Brave search
    strategies.append(("C", f"Brave: `\"{firm}\" site:linkedin.com/company`"))
    # Strategy D: employees cross-check
    emps = emp_by_firm.get(normalize_name(firm), [])
    if emps:
        company_urls = Counter(e.get("source_linkedin_url") for e in emps if e.get("source_linkedin_url"))
        top_url = company_urls.most_common(1)[0][0] if company_urls else None
        strategies.append(("D", f"{len(emps)} scrappy employee profiles found — top source_linkedin_url: {top_url}"))
    else:
        strategies.append(("D", "no scrappy employees for cross-check"))
    return strategies

with OUT_RECON.open("w") as f:
    f.write("# Top 20 Suspect LinkedIn Matches — Reconciliation Strategies\n\n")
    f.write("No API calls yet. For each suspect, listed candidate strategies.\n\n")
    for i, r in enumerate(records[:20], 1):
        f.write(f"## {i}. {r['firm_name']} — score {r['suspect_score']} ({r['source']})\n")
        f.write(f"- Current LinkedIn: {r.get('linkedin_url')}  \n")
        f.write(f"- LK name: {r.get('linkedin_name')} | HQ: {r.get('hq')} | followers: {r.get('followers')} | employees: {r.get('employees')}\n\n")
        for label, note in suggest_strategy(r):
            f.write(f"  - **Strategy {label}**: {note}\n")
        f.write("\n")

# ---------- print report ----------
print(f"Total scored: {len(records)}")
print(f"High (>60):   {len(high)}")
print(f"Medium (40-60): {len(medium)}")
print(f"Low (20-40):  {len(low)}")
print(f"Trusted (<20): {len(ok)}")
print()
print("TOP 10:")
for i, r in enumerate(records[:10], 1):
    print(f"{i:2d}. [{r['suspect_score']:5.1f}] {r['source']:4s} {r['firm_name'][:50]:50s} -> {r.get('linkedin_url')}")
    print(f"     HQ={r.get('hq')} fol={r.get('followers')} emp={r.get('employees')} ind={r.get('industry')}")

print()
print(f"Wrote: {OUT_SUSP}")
print(f"Wrote: {OUT_TOP100}")
print(f"Wrote: {OUT_OK}")
print(f"Wrote: {OUT_RECON}")

# ---------- Sarwa / plankpr sidecheck ----------
print()
print("=== SARWA / plankpr sidecheck ===")
sarwa_reg = None
for r in register:
    if "sarwa" in (r.get("company") or "").lower():
        sarwa_reg = r
        break
if sarwa_reg:
    print(f"ADGM register entry for '{sarwa_reg['company']}':")
    print(f"  pageUrl   : {sarwa_reg.get('pageUrl')}")
    print(f"  address   : {sarwa_reg.get('address')}")
    print(f"  ADGM register HAS NO 'website' field -> plankpr.com is NOT from ADGM register")
else:
    print("Sarwa not found in register")

sarwa_enriched = [r for r in adgm if "sarwa" in (r.get("firm_name") or "").lower()]
for r in sarwa_enriched:
    print(f"APIFY enriched: '{r.get('firm_name')}' -> linkedin={r.get('linkedin_url')} lk_name='{r.get('linkedin_name')}' website={r.get('website')}")
    print(f"  confidence={r.get('match_confidence')} matched_via={r.get('matched_via')}")
print("VERDICT: plankpr.com came from Apify's LinkedIn match (Plank PR company page). The Apify matcher")
print("bound Sarwa Digital Wealth -> linkedin.com/company/plank-pr/ (a London PR agency) — wrong match.")
print("ADGM register itself contains NO website field for any firm.")
