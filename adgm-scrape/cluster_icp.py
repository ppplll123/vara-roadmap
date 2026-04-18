#!/usr/bin/env python3
"""ICP clustering for 515 UAE-regulated firms (ADGM + VARA).

Loads master-data.json, engineers numeric + categorical features, runs
HDBSCAN (with KMeans fallback), characterises clusters, and writes:
  - firms-icp-clusters.json
  - firms-icp-clusters.md
Also patches the master-data.json with cluster_id + cluster_name per firm.
"""
from __future__ import annotations

import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score

try:
    import hdbscan  # type: ignore
    HAVE_HDBSCAN = True
except Exception:
    HAVE_HDBSCAN = False

ROOT = Path("/Users/openclaw/playwright")
MASTER = ROOT / "antweave_roadmap" / "master-data.json"
OUT_JSON = ROOT / "vara_roadmap" / "adgm-scrape" / "firms-icp-clusters.json"
OUT_MD = ROOT / "vara_roadmap" / "adgm-scrape" / "firms-icp-clusters.md"

# ── Country → ISO2 + region mapping (covers all observed HQs) ─────────────
COUNTRY_ALIASES = {
    "united states": "US", "united states of america": "US", "usa": "US",
    "united kingdom": "GB", "uk": "GB", "england": "GB", "scotland": "GB",
    "united arab emirates": "AE", "uae": "AE",
    "switzerland": "CH", "germany": "DE", "france": "FR", "netherlands": "NL",
    "ireland": "IE", "luxembourg": "LU", "belgium": "BE", "italy": "IT",
    "spain": "ES", "sweden": "SE", "denmark": "DK", "norway": "NO", "finland": "FI",
    "portugal": "PT", "austria": "AT", "poland": "PL", "czech republic": "CZ",
    "hungary": "HU", "greece": "GR", "bulgaria": "BG", "romania": "RO",
    "cyprus": "CY", "malta": "MT", "iceland": "IS",
    "singapore": "SG", "hong kong": "HK", "china": "CN", "japan": "JP",
    "south korea": "KR", "korea": "KR", "india": "IN", "pakistan": "PK",
    "australia": "AU", "new zealand": "NZ", "malaysia": "MY", "indonesia": "ID",
    "thailand": "TH", "philippines": "PH", "vietnam": "VN", "taiwan": "TW",
    "saudi arabia": "SA", "qatar": "QA", "bahrain": "BH", "oman": "OM",
    "kuwait": "KW", "jordan": "JO", "lebanon": "LB", "egypt": "EG",
    "israel": "IL", "turkey": "TR", "iraq": "IQ",
    "south africa": "ZA", "nigeria": "NG", "kenya": "KE", "morocco": "MA",
    "brazil": "BR", "argentina": "AR", "mexico": "MX", "chile": "CL",
    "colombia": "CO", "peru": "PE",
    "canada": "CA", "bermuda": "BM", "cayman islands": "KY", "bahamas": "BS",
    "british virgin islands": "VG", "bvi": "VG",
    "jersey": "JE", "guernsey": "GG", "isle of man": "IM", "gibraltar": "GI",
    "mauritius": "MU", "seychelles": "SC", "liechtenstein": "LI", "monaco": "MC",
    "russia": "RU", "ukraine": "UA",
}
REGION_MAP = {
    "AE": "UAE", "SA": "GCC", "QA": "GCC", "BH": "GCC", "OM": "GCC", "KW": "GCC",
    "GB": "UK", "JE": "UK", "GG": "UK", "IM": "UK", "GI": "UK",
    "US": "US", "CA": "US",
    "CH": "EU", "DE": "EU", "FR": "EU", "NL": "EU", "IE": "EU", "LU": "EU",
    "BE": "EU", "IT": "EU", "ES": "EU", "SE": "EU", "DK": "EU", "NO": "EU",
    "FI": "EU", "PT": "EU", "AT": "EU", "PL": "EU", "CZ": "EU", "HU": "EU",
    "GR": "EU", "BG": "EU", "RO": "EU", "CY": "EU", "MT": "EU", "IS": "EU",
    "LI": "EU", "MC": "EU", "RU": "EU", "UA": "EU", "TR": "EU",
    "SG": "APAC", "HK": "APAC", "CN": "APAC", "JP": "APAC", "KR": "APAC",
    "IN": "APAC", "PK": "APAC", "AU": "APAC", "NZ": "APAC", "MY": "APAC",
    "ID": "APAC", "TH": "APAC", "PH": "APAC", "VN": "APAC", "TW": "APAC",
    "JO": "MENA", "LB": "MENA", "EG": "MENA", "IL": "MENA", "IQ": "MENA",
    "MA": "AFRICA", "ZA": "AFRICA", "NG": "AFRICA", "KE": "AFRICA",
    "MU": "AFRICA", "SC": "AFRICA",
    "BR": "LATAM", "AR": "LATAM", "MX": "LATAM", "CL": "LATAM",
    "CO": "LATAM", "PE": "LATAM",
    "BM": "OFFSHORE", "KY": "OFFSHORE", "BS": "OFFSHORE", "VG": "OFFSHORE",
}

# ── Activity taxonomy (map ADGM activity `type` + VARA licence codes) ─────
ACTIVITY_CATEGORY = {
    # ADGM activity types
    "Arranging Deals in Investments": "Advisory",
    "Advising on Investments or Credit": "Advisory",
    "Managing a Collective Investment Fund": "FundMgt",
    "Acting as the Administrator of a Collective Investment Fund": "FundMgt",
    "Managing Assets": "AssetMgt",
    "Arranging Credit": "Credit",
    "Providing Credit": "Credit",
    "Arranging Custody": "Custody",
    "Providing Custody": "Custody",
    "Dealing in Investments as Principal (Matched)": "BrokerDealer",
    "Dealing in Investments as Principal (not matched)": "BrokerDealer",
    "Dealing in Investments as Agent": "BrokerDealer",
    "Operating a Multilateral Trading Facility": "Exchange",
    "Operating a Private Financing Platform": "Exchange",
    "Providing Money Services": "Payments",
    "Currency exchange and Money Remittance": "Payments",
    "Payment Services": "Payments",
    "Accepting Deposits": "Banking",
    "Insurance Intermediation": "Insurance",
    "Insurance Management": "Insurance",
    "Effecting Contracts of Insurance and Carrying Out Contracts of Insurance as a Captive Insurer": "Insurance",
    "Engaging in Islamic Financial Business": "Islamic",
    "Sharia-Compliant Regulated Activities": "Islamic",
    "Operating a Representative Office": "RepOffice",
    "Providing Trust Services": "Trust",
    "Providing Trust Services other than as a Trustee of an express Trust": "Trust",
    "Issuing a Fiat - Referenced Token": "VAIssuance",
    "Providing Third Party Services": "Other",
}
VARA_LICENCE_CATEGORY = {
    "BD": "BrokerDealer",
    "EX": "Exchange",
    "CU": "Custody",
    "M&I": "AssetMgt",
    "AD": "Advisory",
    "LB": "Credit",
    "VA1": "VAIssuance",
}
ALL_CATEGORIES = [
    "Advisory", "AssetMgt", "FundMgt", "BrokerDealer", "Custody", "Exchange",
    "Banking", "Credit", "Insurance", "Islamic", "Payments", "RepOffice",
    "Trust", "VAIssuance", "Other",
]


def extract_country(hq: str | None) -> str:
    if not hq:
        return "UNK"
    s = hq.strip()
    if not s:
        return "UNK"
    # ISO-2 tokens already?
    if len(s) == 2 and s.isalpha():
        return s.upper()
    # Last comma-separated token
    last = s.split(",")[-1].strip().lower()
    if last in COUNTRY_ALIASES:
        return COUNTRY_ALIASES[last]
    # Full-string match
    low = s.lower()
    for k, v in COUNTRY_ALIASES.items():
        if k in low:
            return v
    # 2-letter bare tokens
    if len(last) == 2 and last.isalpha():
        return last.upper()
    return "UNK"


def region_of(iso2: str) -> str:
    return REGION_MAP.get(iso2, "OTHER" if iso2 != "UNK" else "UNKNOWN")


def activity_cats(firm: dict) -> list[str]:
    cats: list[str] = []
    for a in firm.get("acts") or []:
        t = a.get("type") if isinstance(a, dict) else str(a)
        if t and t in ACTIVITY_CATEGORY:
            cats.append(ACTIVITY_CATEGORY[t])
    for lic in firm.get("vara_licences") or []:
        if lic in VARA_LICENCE_CATEGORY:
            cats.append(VARA_LICENCE_CATEGORY[lic])
    return cats


def build_features(firms: list[dict]) -> pd.DataFrame:
    rows = []
    for f in firms:
        emp = f.get("emp") or 0
        fol = f.get("fol") or 0
        acts = f.get("acts") or []
        cond = f.get("cond") or []
        regs = f.get("reg_actions") or []
        iso2 = extract_country(f.get("hq"))
        region = region_of(iso2)
        cats = activity_cats(f)
        cat_counter = Counter(cats)
        row = {
            "id": f.get("id"),
            "name": f.get("n"),
            "src": f.get("src"),
            "emp": emp,
            "fol": fol,
            "emp_log": math.log10(max(1, emp)),
            "fol_log": math.log10(max(1, fol)),
            "n_activities": len(acts) + len(f.get("vara_licences") or []),
            "n_conditions": len(cond),
            "has_reg_action": 1 if regs else 0,
            "is_vara": 1 if f.get("src") == "VARA" else 0,
            "hq_country": iso2,
            "hq_region": region,
        }
        for c in ALL_CATEGORIES:
            row[f"cat_{c}"] = 1 if cat_counter.get(c, 0) > 0 else 0
        rows.append(row)
    return pd.DataFrame(rows)


def build_matrix(df: pd.DataFrame) -> tuple[np.ndarray, list[str]]:
    numeric_cols = ["emp_log", "fol_log", "n_activities", "n_conditions", "has_reg_action", "is_vara"]
    cat_cols = [f"cat_{c}" for c in ALL_CATEGORIES]
    # One-hot encode region
    region_dummies = pd.get_dummies(df["hq_region"], prefix="reg")
    region_cols = list(region_dummies.columns)
    # Concat
    num = df[numeric_cols].to_numpy(dtype=float)
    cat = df[cat_cols].to_numpy(dtype=float)
    reg = region_dummies.to_numpy(dtype=float)
    # Scale numeric only
    scaler = StandardScaler()
    num_s = scaler.fit_transform(num)
    # Give activity category features a reasonable weight so they drive
    # clustering alongside scaled numerics.
    cat_w = 1.2
    reg_w = 0.9
    X = np.hstack([num_s, cat * cat_w, reg * reg_w])
    feat_names = numeric_cols + cat_cols + region_cols
    return X, feat_names


def run_clustering(X: np.ndarray) -> tuple[np.ndarray, str, float]:
    """Return (labels, algorithm, silhouette). Labels: -1 = noise.

    Selection rules:
      1. Prefer HDBSCAN config that yields 4-7 clusters with <=30% noise.
      2. Else fall back to KMeans in [4..7], pick best silhouette.
    """
    hdb_candidates = []
    if HAVE_HDBSCAN:
        configs = []
        for method in ("eom", "leaf"):
            for mcs, ms in [(30, 10), (25, 8), (20, 10), (20, 5), (15, 5), (12, 5), (10, 3)]:
                configs.append((mcs, ms, method))
        for mcs, ms, method in configs:
            clusterer = hdbscan.HDBSCAN(min_cluster_size=mcs, min_samples=ms,
                                         metric="euclidean",
                                         cluster_selection_method=method)
            labels = clusterer.fit_predict(X)
            uniq = [u for u in set(labels) if u != -1]
            n_clusters = len(uniq)
            n_noise = int((labels == -1).sum())
            noise_pct = n_noise / len(labels)
            try:
                mask = labels != -1
                sil = silhouette_score(X[mask], labels[mask]) if n_clusters >= 2 and mask.sum() > n_clusters else -1.0
            except Exception:
                sil = -1.0
            tag = f"HDBSCAN(mcs={mcs},ms={ms},{method})"
            print(f"[{tag}] clusters={n_clusters} noise={n_noise} ({noise_pct:.0%}) silhouette={sil:.3f}")
            if 4 <= n_clusters <= 7 and noise_pct <= 0.30:
                hdb_candidates.append((sil, labels, tag))

    # KMeans sweep — try full-dim and PCA-reduced variants. Silhouette is
    # measured in the same space used to cluster so numbers are comparable.
    km_candidates = []
    for k in [4, 5, 6, 7]:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X)
        try:
            sil = silhouette_score(X, labels)
        except Exception:
            sil = -1.0
        print(f"[KMeans k={k}] silhouette={sil:.3f}")
        km_candidates.append((sil, labels, f"KMeans(k={k})"))

    # PCA-reduced KMeans — denser clusters often separate better in low-dim.
    for n_comp in (8, 6, 5, 4):
        pca = PCA(n_components=min(n_comp, X.shape[1]), random_state=42)
        Xp = pca.fit_transform(X)
        for k in [4, 5, 6, 7]:
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(Xp)
            try:
                sil = silhouette_score(Xp, labels)
            except Exception:
                sil = -1.0
            print(f"[KMeans+PCA{n_comp} k={k}] silhouette={sil:.3f}")
            km_candidates.append((sil, labels, f"KMeans+PCA{n_comp}(k={k})"))

    # Agglomerative (Ward) on PCA(8)
    try:
        pca8 = PCA(n_components=min(8, X.shape[1]), random_state=42).fit_transform(X)
        for k in [4, 5, 6, 7]:
            ac = AgglomerativeClustering(n_clusters=k, linkage="ward")
            labels = ac.fit_predict(pca8)
            try:
                sil = silhouette_score(pca8, labels)
            except Exception:
                sil = -1.0
            print(f"[Agglomerative(Ward)+PCA8 k={k}] silhouette={sil:.3f}")
            km_candidates.append((sil, labels, f"Agglomerative+PCA8(k={k})"))
    except Exception as e:
        print(f"Agglomerative skipped: {e}")

    # Prefer HDBSCAN if any acceptable candidate exists
    if hdb_candidates:
        hdb_candidates.sort(key=lambda t: -t[0])
        sil, labels, algo = hdb_candidates[0]
        print(f"→ Chose HDBSCAN candidate {algo} (silhouette {sil:.3f})")
        return labels, algo, sil

    km_candidates.sort(key=lambda t: -t[0])
    sil, labels, algo = km_candidates[0]
    print(f"→ No acceptable HDBSCAN, using {algo} (silhouette {sil:.3f})")
    return labels, algo, sil


def cluster_entropy(values: list[str]) -> float:
    if not values:
        return 0.0
    cnt = Counter(values)
    total = sum(cnt.values())
    return -sum((v / total) * math.log2(v / total) for v in cnt.values() if v > 0)


def characterise(df: pd.DataFrame, labels: np.ndarray, firms: list[dict]) -> list[dict]:
    df = df.copy()
    df["cluster"] = labels
    clusters = []
    for cid in sorted(set(labels)):
        sub = df[df["cluster"] == cid]
        sub_firms = [firms[i] for i in sub.index.tolist()]
        all_acts_types = []
        all_cats = []
        for f in sub_firms:
            for a in f.get("acts") or []:
                t = a.get("type") if isinstance(a, dict) else str(a)
                if t:
                    all_acts_types.append(t)
            for lic in f.get("vara_licences") or []:
                all_acts_types.append(f"VARA:{lic}")
            all_cats.extend(activity_cats(f))
        top_acts = Counter(all_acts_types).most_common(5)
        top_cats = Counter(all_cats).most_common(3)
        # Top 5 firms by employees
        by_emp = sorted(sub_firms, key=lambda f: -(f.get("emp") or 0))[:5]
        top_firms_named = [{"name": f.get("n"), "emp": f.get("emp") or 0, "id": f.get("id")} for f in by_emp]
        # heterogeneity
        emp_std = float(sub["emp_log"].std() or 0.0)
        nact_std = float(sub["n_activities"].std() or 0.0)
        region_entropy = cluster_entropy(sub["hq_region"].tolist())
        # Normalise: emp_log ~ 0..5 → /5; nact ~ 0..6 → /6; entropy ~ 0..3 → /3
        het = (min(1.0, emp_std / 2.0) + min(1.0, nact_std / 3.0) + min(1.0, region_entropy / 2.5)) / 3.0
        hom = 1.0 - het
        mode_src = Counter(sub["src"].tolist()).most_common(1)[0][0]
        mode_region = Counter(sub["hq_region"].tolist()).most_common(1)[0][0]
        cluster = {
            "id": int(cid),
            "size": int(len(sub)),
            "is_noise": bool(cid == -1),
            "features": {
                "mean_emp": float(sub["emp"].mean()),
                "median_emp": float(sub["emp"].median()),
                "mean_fol": float(sub["fol"].mean()),
                "mean_n_activities": float(sub["n_activities"].mean()),
                "mean_n_conditions": float(sub["n_conditions"].mean()),
                "pct_with_reg_action": float(sub["has_reg_action"].mean()),
                "pct_vara": float(sub["is_vara"].mean()),
                "mode_src": mode_src,
                "mode_region": mode_region,
                "region_distribution": dict(Counter(sub["hq_region"].tolist())),
                "country_top3": dict(Counter(sub["hq_country"].tolist()).most_common(3)),
            },
            "top_activities": [{"activity": a, "count": c} for a, c in top_acts],
            "top_categories": [{"category": a, "count": c} for a, c in top_cats],
            "top_firms": top_firms_named,
            "heterogeneity": round(het, 3),
            "homogeneity": round(hom, 3),
        }
        clusters.append(cluster)
    return clusters


def auto_name(cluster: dict) -> str:
    """Rule-based heuristic ICP names. No LLM call needed — cheap & deterministic."""
    if cluster["is_noise"]:
        return "Edge Cases / Noise"
    f = cluster["features"]
    top_cats = [c["category"] for c in cluster["top_categories"]] or ["Mixed"]
    primary_cat = top_cats[0]
    region = f["mode_region"]
    mean_emp = f["mean_emp"]
    pct_vara = f["pct_vara"]
    pct_reg = f["pct_with_reg_action"]

    cat_label = {
        "Advisory": "Advisory",
        "AssetMgt": "Asset Management",
        "FundMgt": "Fund Management",
        "BrokerDealer": "Broker-Dealer",
        "Custody": "Custody",
        "Exchange": "Exchange / Trading Venue",
        "Banking": "Banking",
        "Credit": "Credit / Lending",
        "Insurance": "Insurance",
        "Islamic": "Islamic Finance",
        "Payments": "Payments",
        "RepOffice": "Representative Office",
        "Trust": "Trust Services",
        "VAIssuance": "VA Issuance",
        "Other": "Diversified",
        "Mixed": "Mixed",
    }.get(primary_cat, primary_cat)

    if mean_emp >= 5000:
        size = "Global"
    elif mean_emp >= 500:
        size = "Large"
    elif mean_emp >= 50:
        size = "Mid-Cap"
    else:
        size = "Boutique"

    region_label = {
        "UAE": "UAE-HQ",
        "UK": "UK-HQ",
        "US": "US-HQ",
        "EU": "EU-HQ",
        "APAC": "APAC-HQ",
        "GCC": "GCC-HQ",
        "OFFSHORE": "Offshore-HQ",
        "MENA": "MENA-HQ",
        "AFRICA": "Africa-HQ",
        "LATAM": "LATAM-HQ",
        "OTHER": "Global",
        "UNKNOWN": "Undisclosed-HQ",
    }.get(region, region)

    suffix = ""
    if pct_vara > 0.6:
        suffix = " (VARA-crypto)"
    if pct_reg > 0.3:
        suffix += " ⚠flagged"

    return f"{size} {cat_label} — {region_label}{suffix}"


def outreach_angle(cluster: dict) -> str:
    if cluster["is_noise"]:
        return "Manual review — firms that didn't fit the main ICP clusters."
    f = cluster["features"]
    cats = [c["category"] for c in cluster["top_categories"]]
    primary = cats[0] if cats else "Mixed"
    pct_vara = f["pct_vara"]
    pct_reg = f["pct_with_reg_action"]
    mean_emp = f["mean_emp"]

    if pct_vara > 0.6:
        return ("VARA-regulated crypto/VA operators. Lead with ERC-3643 permissioned-token readiness, "
                "Rulebook cross-mapping, and RI/OC placement support.")
    if primary in ("FundMgt", "AssetMgt") and mean_emp > 200:
        return ("Established asset/fund managers. Pitch tokenized-fund distribution rails, secondary-market "
                "structuring, and investor-onboarding KYC tooling.")
    if primary == "Advisory":
        return ("Investment advisory firms — often lean teams. Offer compliance-as-a-service, "
                "regulated-activity gap analysis, and referral partnerships on RWA tokenization.")
    if primary == "BrokerDealer":
        return ("Broker-dealers / market makers. Position as venue-neutral liquidity partner and "
                "pitch regulated VA order-flow plumbing.")
    if primary in ("Custody",):
        return ("Custodians — regulatory-sensitive. Sell key-management + sub-custody integrations "
                "and Rulebook-aligned attestation reports.")
    if primary == "Exchange":
        return ("Trading venues / MTFs. Pitch listing pipeline of tokenized RWAs and cross-venue connectivity.")
    if primary == "Insurance":
        return ("Insurers — conservative buyers. Lead with captive structures, tokenized insurance-linked "
                "securities, and Solvency/Basel alignment notes.")
    if primary == "Banking":
        return ("Banks — enterprise procurement cycle. Target innovation/digital-assets teams with a "
                "CBDC / tokenised-deposits narrative.")
    if pct_reg > 0.3:
        return ("Firms with prior enforcement history — lead with remediation/uplift framing, "
                "not net-new licence applications.")
    return "General-purpose outreach — lead with UAE regulatory landscape briefing + tokenization demo."


def main() -> int:
    print(f"Loading {MASTER}")
    data = json.loads(MASTER.read_text())
    firms = data["firms"]
    print(f"Loaded {len(firms)} firms")

    df = build_features(firms)
    X, feat_names = build_matrix(df)
    print(f"Feature matrix: {X.shape} · features: {len(feat_names)}")

    labels, algo, sil = run_clustering(X)
    uniq = sorted(set(labels))
    print(f"Chosen: {algo} · silhouette={sil:.3f} · clusters={len([u for u in uniq if u != -1])} · noise={(labels==-1).sum()}")

    clusters = characterise(df, labels, firms)
    for c in clusters:
        c["name"] = auto_name(c)
        c["outreach_angle"] = outreach_angle(c)

    # firm_cluster_map
    firm_cluster_map = {}
    for i, f in enumerate(firms):
        firm_cluster_map[f["id"]] = int(labels[i])

    # Order clusters: non-noise first by size desc, noise last
    clusters_sorted = sorted(
        clusters,
        key=lambda c: (1 if c["is_noise"] else 0, -c["size"]),
    )

    out = {
        "algorithm": algo,
        "silhouette": round(float(sil), 4),
        "n_firms": len(firms),
        "n_clusters": len([c for c in clusters if not c["is_noise"]]),
        "n_noise": sum(c["size"] for c in clusters if c["is_noise"]),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "feature_names": feat_names,
        "clusters": clusters_sorted,
        "firm_cluster_map": firm_cluster_map,
    }
    OUT_JSON.write_text(json.dumps(out, indent=2, default=str))
    print(f"Wrote {OUT_JSON}")

    # ── Markdown report ────────────────────────────────────────────────
    md = []
    md.append(f"# UAE Regulated Firms — ICP Clustering Report\n")
    md.append(f"_Generated: {out['generated_at']}_\n")
    md.append(f"- **Firms analysed:** {out['n_firms']}")
    md.append(f"- **Algorithm:** `{algo}`")
    md.append(f"- **Silhouette score:** {out['silhouette']:.3f}")
    md.append(f"- **Clusters:** {out['n_clusters']} + {out['n_noise']} noise/edge firms\n")
    md.append("## Cluster summary\n")
    md.append("| ID | Name | Size | Region | Mean emp | % VARA | % flagged | Top activity |")
    md.append("|---:|------|-----:|--------|---------:|-------:|----------:|--------------|")
    for c in clusters_sorted:
        f = c["features"]
        ta = c["top_activities"][0]["activity"] if c["top_activities"] else "—"
        md.append(f"| {c['id']} | **{c['name']}** | {c['size']} | {f['mode_region']} | "
                  f"{f['mean_emp']:.0f} | {f['pct_vara']:.0%} | {f['pct_with_reg_action']:.0%} | {ta} |")
    md.append("")
    for c in clusters_sorted:
        f = c["features"]
        md.append(f"### Cluster {c['id']} — {c['name']} ({c['size']} firms)")
        md.append(f"- Mean employees: {f['mean_emp']:.0f} · median: {f['median_emp']:.0f} · mean followers: {f['mean_fol']:.0f}")
        md.append(f"- Mean regulated activities: {f['mean_n_activities']:.1f} · conditions: {f['mean_n_conditions']:.1f}")
        md.append(f"- % VARA: {f['pct_vara']:.0%} · % with prior reg action: {f['pct_with_reg_action']:.0%}")
        md.append(f"- Dominant region: **{f['mode_region']}** · region mix: {f['region_distribution']}")
        md.append(f"- Top countries: {f['country_top3']}")
        top_acts_str = ", ".join(f"{a['activity']} ({a['count']})" for a in c["top_activities"][:3]) or "—"
        md.append(f"- Top activities: {top_acts_str}")
        top_cats_str = ", ".join(f"{a['category']} ({a['count']})" for a in c["top_categories"]) or "—"
        md.append(f"- Top categories: {top_cats_str}")
        md.append(f"- Homogeneity: **{c['homogeneity']:.2f}** · Heterogeneity: {c['heterogeneity']:.2f}")
        md.append(f"- Representative firms (by employees): " + ", ".join(f"{x['name']} ({x['emp']:,})" for x in c["top_firms"]))
        md.append(f"- **Outreach angle:** {c['outreach_angle']}")
        md.append("")
    OUT_MD.write_text("\n".join(md))
    print(f"Wrote {OUT_MD}")

    # ── Patch master-data.json in-place with cluster_id + cluster_name ─
    name_by_id = {c["id"]: c["name"] for c in clusters_sorted}
    for i, f in enumerate(firms):
        cid = int(labels[i])
        f["cluster_id"] = cid
        f["cluster_name"] = name_by_id.get(cid, "Edge Cases / Noise")
    MASTER.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    print(f"Patched {MASTER} with cluster_id + cluster_name")

    return 0


if __name__ == "__main__":
    sys.exit(main())
