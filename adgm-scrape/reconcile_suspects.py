#!/usr/bin/env python3
"""
reconcile_suspects.py
---------------------
Fix wrong LinkedIn company matches for suspect firms (score >= 40).

ZERO API CALLS. Strategies in priority order:

A. Employee-headline validation (scrappy-employees-flat.json)
   The scrappy scrape already reached a LinkedIn URL and pulled ~15 employees
   per firm. Each employee has `headline` + `snippet`. If NONE of those
   mention the target firm name tokens, the URL is confirmed WRONG (we
   cannot rely on it). If a MAJORITY mention it, URL is validated OK.
   Note: this does NOT yield the correct URL on its own (all employees
   share the same source URL), but it confirms / refutes the current link.

B. Firm-website scrape cache + local curl fallback
   - Look at firecrawl cache files in `.firecrawl/*.md` for pre-scraped
     firm homepage / about-page markdown. Grep for `linkedin.com/company/*`.
   - If cache miss, attempt `curl --max-time 3` on the firm website recorded
     in `firms-linkedin-enriched.json` / `entities-linkedin-enriched.json`
     / master-data.json. Grep for `linkedin.com/company/*`.
   - Website domain must not appear to belong to the WRONG LinkedIn match
     (reject if website derived from the bad enrichment).
   - First hit (not equal to currently stored bad URL) is adopted.

C. Mark as unverified
   If neither A nor B yields a trusted URL, null out the linkedin.url,
   .followers, .employees fields and set linkedin.status = "unverified".

Outputs:
  - linkedin-reconciled.json          per-firm decision w/ old/new/strategy/conf
  - linkedin-reconciled-summary.md    human-readable roll-up
  - master-data-patches.json         list of {firm_id, field, old, new} patches
  - master-data.json.bak             backup before writing
  - antweave_roadmap/master-data.json patched in place
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Repo root = two parents up from this file (.../vara_roadmap/adgm-scrape/reconcile_suspects.py)
ROOT = Path(__file__).resolve().parents[2]
ADGM_SCRAPE = ROOT / "vara_roadmap/adgm-scrape"
SUSPECTS_F = ADGM_SCRAPE / "linkedin-suspects.json"
EMP_FLAT_F = ADGM_SCRAPE / "scrappy-employees-flat.json"
FULL_REG_F = ADGM_SCRAPE / "full-register-all-tabs.json"
ADGM_ENRICH_F = ADGM_SCRAPE / "firms-linkedin-enriched.json"
VARA_ENRICH_F = ROOT / "vara_roadmap/entities-linkedin-enriched.json"
FIRECRAWL_DIR = ROOT / ".firecrawl"
MASTER_F = ROOT / "antweave_roadmap/master-data.json"
MASTER_BAK = ROOT / "antweave_roadmap/master-data.json.bak"

OUT_RECON = ADGM_SCRAPE / "linkedin-reconciled.json"
OUT_SUMMARY = ADGM_SCRAPE / "linkedin-reconciled-summary.md"
OUT_PATCHES = ADGM_SCRAPE / "master-data-patches.json"

SUSPECT_THRESHOLD = 40

# keyword tokens we strip from firm names when building match tokens
STOP_TOKENS = {
    "limited", "ltd", "ltd.", "l.l.c", "llc", "l.l.c.", "plc", "inc", "inc.",
    "company", "co", "co.", "corporation", "corp", "corp.", "the", "middle",
    "east", "me", "mena", "dmcc", "fze", "fzc", "fz-llc", "pjsc", "p.j.s.c.",
    "p.j.s.c", "sa", "s.a", "s.a.", "gmbh", "ag", "sarl", "bv", "nv",
    "holdings", "holding", "group", "international", "global", "pty",
    "capital", "advisors", "advisory", "partners", "management",
    "technology", "technologies", "solutions", "services", "services.",
    "financial", "finance", "trading", "trust", "trustees", "fund",
    "funds", "investments", "investment", "asset", "assets",
    "(ad)", "ad", "(a)", "a", "(me)", "me", "(middle", "east)",
    "private", "public",
}

# Generic tokens that are too common to be the sole evidence for a firm match.
# A slug must contain at least one non-generic firm token for Strategy B to accept it.
GENERIC_TOKENS = {
    "digital", "invest", "invests", "wealth", "wealthy", "bank", "banking",
    "broker", "brokers", "markets", "market", "fintech", "crypto",
    "pay", "payments", "tech", "money", "venture", "ventures",
    "asset", "assets", "analytics", "analysis", "solutions",
    "ltd", "group", "holdings", "capital", "partners", "management",
    "middle", "east", "global", "international", "financial", "finance",
}

# ---------- helpers ----------


def norm_name(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[\&\(\)\.,'\"`/]", " ", s)
    s = re.sub(r"[^a-z0-9\- ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def name_tokens(name: str) -> List[str]:
    toks = [t for t in norm_name(name).split() if t and t not in STOP_TOKENS]
    # Require length >= 3 (drop "a", "ad", "me", ...)
    toks = [t for t in toks if len(t) >= 3]
    return toks


def strip_url_slug(url: str) -> str:
    m = re.search(r"linkedin\.com/company/([a-zA-Z0-9\-_%]+)", url or "")
    return m.group(1).lower() if m else ""


def hay(s: str) -> str:
    return (s or "").lower()


# ---------- Strategy A: employee headline validation ----------


def build_emp_index(emps: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    idx: Dict[str, List[Dict[str, Any]]] = {}
    for e in emps:
        sf = e.get("source_firm") or ""
        idx.setdefault(sf.strip().lower(), []).append(e)
    return idx


def employees_match_name(
    firm_name: str, current_url: str, employees: List[Dict[str, Any]]
) -> Tuple[int, int, str]:
    """
    Return (mentions_firm_count, total_employees, consensus_status).
    consensus_status in {"confirmed_correct", "confirmed_wrong", "insufficient"}.
    """
    if not employees:
        return 0, 0, "insufficient"
    tokens = name_tokens(firm_name)
    if not tokens:
        return 0, len(employees), "insufficient"
    # Primary distinctive token: longest
    primary = max(tokens, key=len)
    # Also collect the URL slug's tokens (they indicate the WRONG firm)
    wrong_slug = strip_url_slug(current_url)
    wrong_tokens = [t for t in re.split(r"[\-_]", wrong_slug) if len(t) >= 4]

    firm_hits = 0
    wrong_hits = 0
    for e in employees:
        text = " ".join(
            [
                hay(e.get("headline")),
                hay(e.get("snippet")),
                hay(e.get("company")),
            ]
        )
        if primary in text:
            firm_hits += 1
        for wt in wrong_tokens:
            if wt and wt not in tokens and wt in text:
                wrong_hits += 1
                break

    total = len(employees)
    ratio_firm = firm_hits / total
    ratio_wrong = wrong_hits / total

    if ratio_firm >= 0.40 and ratio_firm > ratio_wrong:
        return firm_hits, total, "confirmed_correct"
    if ratio_wrong >= 0.40 and ratio_wrong > ratio_firm:
        return firm_hits, total, "confirmed_wrong"
    if total >= 3 and ratio_firm < 0.10:
        # Nobody mentions the firm -> very likely wrong
        return firm_hits, total, "confirmed_wrong"
    return firm_hits, total, "insufficient"


# ---------- Strategy B: website scrape ----------

LINKEDIN_CO_RX = re.compile(
    r"linkedin\.com/company/([a-zA-Z0-9\-_%]+)", flags=re.IGNORECASE
)


def load_firecrawl_cache() -> Dict[str, List[str]]:
    """
    Map filename_stem -> list of linkedin company slugs found in that file.
    """
    cache: Dict[str, List[str]] = {}
    if not FIRECRAWL_DIR.exists():
        return cache
    for p in FIRECRAWL_DIR.iterdir():
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".md", ".html", ".txt"}:
            continue
        try:
            text = p.read_text(errors="ignore")
        except Exception:
            continue
        slugs = set(s.lower().rstrip("/") for s in LINKEDIN_CO_RX.findall(text))
        if slugs:
            cache[p.stem.lower()] = sorted(slugs)
    return cache


def distinctive_tokens(firm_name: str) -> List[str]:
    """Tokens useful for unique firm identification (strip stop + generic tokens)."""
    toks = name_tokens(firm_name)
    return [t for t in toks if t not in GENERIC_TOKENS and len(t) >= 3]


def _slug_has_distinctive_token(slug: str, firm_name: str) -> bool:
    """Slug must share at least one DISTINCTIVE firm-name token. Reject numeric slugs."""
    if re.fullmatch(r"\d+", slug):
        return False
    disto = distinctive_tokens(firm_name)
    if not disto:
        return False
    slug_norm = re.sub(r"[^a-z0-9]", "", slug.lower())
    for t in disto:
        if len(t) >= 3 and t.replace(" ", "") in slug_norm:
            return True
    return False


def match_firecrawl_for_firm(
    firm_name: str, cache: Dict[str, List[str]]
) -> Tuple[List[str], List[str]]:
    """Return (slugs_found, cache_files_matched) for files whose stem strongly matches firm name.

    Rules to avoid false positives (e.g. 'Rudo Digital Wealth' -> laser-digital):
      - Use DISTINCTIVE tokens only (drop generic words like digital/invest/wealth).
      - Stem (cache file name) must contain a distinctive token as a whole word.
      - If only one distinctive token matches and it is <6 chars, require a
        SECOND firm token (generic or distinctive) also present in the stem.
      - Kept slugs must share a distinctive token with the firm.
    """
    disto = distinctive_tokens(firm_name)
    all_toks = name_tokens(firm_name)
    if not disto:
        return [], []
    primary = max(disto, key=len)
    if len(primary) < 3:
        return [], []
    hits: List[str] = []
    files: List[str] = []
    for stem, slugs in cache.items():
        stem_words = [w for w in re.split(r"[^a-z0-9]+", stem.lower()) if w]
        matched = [t for t in disto if t in stem_words]
        if not matched:
            continue
        # If only a single short distinctive token matched, require secondary evidence
        # (another firm token in the stem). Multiple distinctive tokens matching
        # is already strong evidence even if each is short (e.g. 'ctrl-alt').
        if len(matched) < 2 and max(len(t) for t in matched) < 6:
            secondary = [t for t in all_toks if t not in matched and t in stem_words]
            if not secondary:
                continue
        kept_slugs = [s for s in slugs if _slug_has_distinctive_token(s, firm_name)]
        if kept_slugs:
            hits.extend(kept_slugs)
            files.append(stem)
    seen = set()
    unique_hits = []
    for h in hits:
        if h not in seen:
            unique_hits.append(h)
            seen.add(h)
    return unique_hits, files


def curl_linkedin_slug(url: str, timeout: int = 3) -> List[str]:
    """Run curl -Ls --max-time T url, grep for linkedin.com/company/<slug>. Returns slugs."""
    if not url:
        return []
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        res = subprocess.run(
            [
                "curl",
                "-L",
                "-s",
                "--max-time",
                str(timeout),
                "-A",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127 Safari/537.36",
                url,
            ],
            capture_output=True,
            timeout=timeout + 2,
        )
    except Exception:
        return []
    body = res.stdout.decode(errors="ignore") if res.stdout else ""
    slugs = [s.lower().rstrip("/") for s in LINKEDIN_CO_RX.findall(body)]
    # dedupe preserve order
    out: List[str] = []
    seen: set[str] = set()
    for s in slugs:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def filter_curl_slugs_by_firm(slugs: List[str], firm_name: str) -> List[str]:
    return [s for s in slugs if _slug_has_distinctive_token(s, firm_name)]


# ---------- Master data helpers ----------


def find_firm_in_master(master: Dict[str, Any], firm_name: str, source: str) -> Optional[Dict[str, Any]]:
    fn_norm = norm_name(firm_name)
    for f in master["firms"]:
        if norm_name(f.get("n", "")) == fn_norm and (f.get("src") == source or not source):
            return f
    # fallback: ignore source
    for f in master["firms"]:
        if norm_name(f.get("n", "")) == fn_norm:
            return f
    return None


# ---------- main ----------


def main() -> int:
    print("Loading inputs ...")
    suspects_doc = json.loads(SUSPECTS_F.read_text())
    emps = json.loads(EMP_FLAT_F.read_text())
    master = json.loads(MASTER_F.read_text())

    # website lookups from enrichment files (may be WRONG too, but keep as seed URL)
    adgm_enrich = json.loads(ADGM_ENRICH_F.read_text())
    vara_enrich = json.loads(VARA_ENRICH_F.read_text())
    website_lookup: Dict[str, str] = {}
    for row in adgm_enrich:
        k = norm_name(row.get("firm_name", ""))
        if k and row.get("website"):
            website_lookup[k] = row["website"]
    for row in vara_enrich:
        k = norm_name(row.get("entity", ""))
        if k and row.get("website"):
            website_lookup[k] = row["website"]

    emp_idx = build_emp_index(emps)
    firecrawl_cache = load_firecrawl_cache()
    print(
        f"  suspects={suspects_doc['total_scored']}, employees={len(emps)}, firms={len(master['firms'])}, "
        f"firecrawl_files={len(firecrawl_cache)}"
    )

    suspects = [r for r in suspects_doc["records"] if r.get("suspect_score", 0) >= SUSPECT_THRESHOLD]
    suspects.sort(key=lambda r: -r["suspect_score"])
    print(f"Suspects @ score >= {SUSPECT_THRESHOLD}: {len(suspects)}")

    decisions: List[Dict[str, Any]] = []
    patches: List[Dict[str, Any]] = []

    strat_counts = Counter()

    for s in suspects:
        firm = s["firm_name"]
        src = s["source"]
        old_url = s["linkedin_url"] or ""
        old_slug = strip_url_slug(old_url)

        firm_rec = find_firm_in_master(master, firm, src)
        firm_id = firm_rec["id"] if firm_rec else None

        emp_matches = emp_idx.get(firm.strip().lower(), [])
        firm_hits, total_emps, consensus = employees_match_name(firm, old_url, emp_matches)

        # Strategy B: firecrawl cache
        fc_slugs, fc_files = match_firecrawl_for_firm(firm, firecrawl_cache)
        # filter: drop the already-wrong slug and obvious non-company noises
        fc_candidates = [s for s in fc_slugs if s and s != old_slug]

        # Strategy B: fallback to live curl of firm website
        website = website_lookup.get(norm_name(firm)) or (firm_rec.get("web") if firm_rec else "")
        web_slugs: List[str] = []
        if not fc_candidates and website:
            raw_web_slugs = curl_linkedin_slug(website, timeout=3)
            raw_web_slugs = [s for s in raw_web_slugs if s and s != old_slug]
            # Only keep slugs that share a firm-name token and aren't numeric LinkedIn redirects
            web_slugs = filter_curl_slugs_by_firm(raw_web_slugs, firm)

        # Decide
        new_url: Optional[str] = None
        strategy: str = ""
        confidence: int = 0
        rationale_bits: List[str] = []

        if fc_candidates:
            new_url = f"https://www.linkedin.com/company/{fc_candidates[0]}/"
            strategy = "B_firecrawl"
            confidence = 80
            rationale_bits.append(
                f"firecrawl_files={fc_files}, linkedin_slugs={fc_candidates[:3]}"
            )
        elif web_slugs:
            new_url = f"https://www.linkedin.com/company/{web_slugs[0]}/"
            strategy = "B_curl"
            confidence = 70
            rationale_bits.append(f"curl({website}) -> {web_slugs[:3]}")
        elif consensus == "confirmed_correct":
            # Keep the URL — Strategy A validates it; do not patch.
            strategy = "A_keep"
            confidence = 60
            rationale_bits.append(
                f"employees_validate: {firm_hits}/{total_emps} mention firm tokens"
            )
        elif consensus == "confirmed_wrong":
            strategy = "C_null"
            confidence = 70
            rationale_bits.append(
                f"employees_refute: {firm_hits}/{total_emps} mention firm tokens"
            )
        else:
            # Heuristic: if suspect_score is VERY high (>=65) and no proof, null it.
            if s["suspect_score"] >= 65:
                strategy = "C_null"
                confidence = 50
                rationale_bits.append(
                    f"high suspect_score={s['suspect_score']}, no Strategy A/B evidence"
                )
            else:
                strategy = "C_keep_unverified"
                confidence = 20
                rationale_bits.append(
                    f"no evidence either way; score={s['suspect_score']}"
                )

        decision = {
            "firm_name": firm,
            "source": src,
            "firm_id": firm_id,
            "suspect_score": s["suspect_score"],
            "old_url": old_url,
            "new_url": new_url,
            "strategy": strategy,
            "confidence": confidence,
            "rationale": "; ".join(rationale_bits),
            "firecrawl_hits": fc_candidates,
            "curl_hits": web_slugs,
            "employee_validation": {
                "firm_hits": firm_hits,
                "total": total_emps,
                "consensus": consensus,
            },
        }
        decisions.append(decision)
        strat_counts[strategy] += 1

        # Build patches
        if firm_rec is None:
            continue
        if strategy.startswith("B_"):
            if firm_rec.get("url") != new_url:
                patches.append(
                    {
                        "firm_id": firm_id,
                        "field": "url",
                        "old_value": firm_rec.get("url"),
                        "new_value": new_url,
                        "strategy": strategy,
                        "confidence": confidence,
                    }
                )
            # Clear stale follower/employee counts that came from the WRONG company
            for field in ("fol", "emp", "hq"):
                if firm_rec.get(field) not in (None, "", 0):
                    patches.append(
                        {
                            "firm_id": firm_id,
                            "field": field,
                            "old_value": firm_rec.get(field),
                            "new_value": None,
                            "strategy": strategy,
                            "confidence": confidence,
                        }
                    )
        elif strategy == "C_null":
            for field in ("url", "fol", "emp", "hq"):
                if firm_rec.get(field) not in (None, "", 0):
                    patches.append(
                        {
                            "firm_id": firm_id,
                            "field": field,
                            "old_value": firm_rec.get(field),
                            "new_value": None,
                            "strategy": strategy,
                            "confidence": confidence,
                        }
                    )
            patches.append(
                {
                    "firm_id": firm_id,
                    "field": "linkedin_status",
                    "old_value": firm_rec.get("linkedin_status"),
                    "new_value": "unverified",
                    "strategy": strategy,
                    "confidence": confidence,
                }
            )

    # ---------- write outputs ----------
    OUT_RECON.write_text(
        json.dumps(
            {
                "threshold": SUSPECT_THRESHOLD,
                "total_suspects": len(suspects),
                "strategy_counts": dict(strat_counts),
                "decisions": decisions,
            },
            indent=2,
        )
    )
    OUT_PATCHES.write_text(json.dumps(patches, indent=2))

    # Summary markdown
    lines = [
        "# LinkedIn Reconciliation Summary",
        "",
        f"- Suspect threshold: {SUSPECT_THRESHOLD}",
        f"- Total suspects processed: {len(suspects)}",
        f"- Total master-data patches: {len(patches)}",
        "",
        "## Strategy counts",
    ]
    for k, v in strat_counts.most_common():
        lines.append(f"- `{k}`: {v}")
    lines += ["", "## Per-firm decisions", ""]
    lines.append(
        "| score | source | firm | strategy | conf | old_url -> new_url |"
    )
    lines.append("|---|---|---|---|---|---|")
    for d in decisions:
        old = (d["old_url"] or "")[-60:]
        new = d["new_url"] or "-"
        new = new[-60:] if new != "-" else new
        lines.append(
            f"| {d['suspect_score']} | {d['source']} | {d['firm_name']} | "
            f"{d['strategy']} | {d['confidence']} | `{old}` -> `{new}` |"
        )
    lines += ["", "## Notable before/after", ""]
    ranked = sorted(decisions, key=lambda x: -x["suspect_score"])[:10]
    for d in ranked:
        lines.append(f"### {d['firm_name']} ({d['source']}, score {d['suspect_score']})")
        lines.append(f"- strategy: **{d['strategy']}** (confidence {d['confidence']})")
        lines.append(f"- old: {d['old_url']}")
        lines.append(f"- new: {d['new_url']}")
        lines.append(f"- rationale: {d['rationale']}")
        lines.append("")
    OUT_SUMMARY.write_text("\n".join(lines))

    # ---------- backup + apply patches ----------
    if patches:
        MASTER_BAK.write_text(MASTER_F.read_text())
        firm_by_id: Dict[str, Dict[str, Any]] = {f["id"]: f for f in master["firms"]}
        applied = 0
        for p in patches:
            fr = firm_by_id.get(p["firm_id"])
            if fr is None:
                continue
            fr[p["field"]] = p["new_value"]
            applied += 1
        master.setdefault("_meta", {})["linkedin_reconcile"] = {
            "threshold": SUSPECT_THRESHOLD,
            "patches_applied": applied,
            "strategy_counts": dict(strat_counts),
        }
        MASTER_F.write_text(json.dumps(master, indent=2))
        print(f"Applied {applied} patches to {MASTER_F}")
    else:
        print("No patches to apply.")

    print("Strategy counts:", dict(strat_counts))
    print(f"Outputs:\n  {OUT_RECON}\n  {OUT_SUMMARY}\n  {OUT_PATCHES}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
