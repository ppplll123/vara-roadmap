# Nulled LinkedIn URLs — Firecrawl Recovery

Date: 2026-04-18
Method: `firecrawl search "<firm name> linkedin" --limit 5 --json` (plus targeted follow-ups)
Raw results saved in `/Users/openclaw/playwright/.firecrawl/nulled/*.json` and combined in `nulled-firms-linkedin-search.json`.
Credits consumed: ~36 / 500.

## Summary

| Firm | Found LinkedIn URL | Confidence | Reason |
|------|-------------------|------------|--------|
| First Abu Dhabi Bank P.J.S.C. | https://www.linkedin.com/company/first-abu-dhabi-bank | HIGH | Top result, exact brand match, HQ Abu Dhabi |
| Wahed Invest Limited | https://www.linkedin.com/company/wahedinvest | HIGH | Top result; "Wahed" global Islamic finance brand; ADGM firm is UAE branch of wahed.com |
| Barrenjoey Markets Pty Limited | https://au.linkedin.com/company/barrenjoey | HIGH | Australian parent's LinkedIn; ADGM firm is "Barrenjoey Markets Pty Limited ADGM Branch" per barrenjoey.com/adgm-privacy-statement/ |
| Rudo Digital Wealth Private Limited | https://www.linkedin.com/company/rudo-wealth-financial-services-and-investment | HIGH | Top result ("RuDo Wealth"); UAE job posting for MLRO confirms ADGM connection |
| MSA Capital ETM LTD | https://www.linkedin.com/company/msacap | MEDIUM | Parent brand MSA Capital; ADGM firm email `walid@mscap.com` links to msacap; ETM is the MENA fund vehicle within MSA Capital |
| Batelco Financial Services Ltd | https://www.linkedin.com/company/batelco | MEDIUM | Parent Bahrain Telecom; no separate "Financial Services" page found |
| First Abu Dhabi Bank (duplicate row — see above) |  |  |  |
| Daliz Finance Ltd | (skip — kept null) | LOW | `linkedin.com/company/daliz-finance` previously flagged as wrong (Ukraine). Could not cheaply confirm true Abu Dhabi page is different. Safer to leave null. |
| VIZIER ASSET MANAGEMENT COMPANY LIMITED | (skip — kept null) | NONE | No LinkedIn company page found; only employee profiles. |
| MRC (MIDDLE EAST) LIMITED | (skip — kept null) | NONE | Malaysian Rubber Council's ME rep office — not on LinkedIn. |
| NewReef Capital Limited | (skip — kept null) | NONE | Has website newreef.ae but no LinkedIn company page surfaced. |

## Recovery outcome

- **6 HIGH/MEDIUM-confidence URLs recovered** (FAB, Wahed, Barrenjoey, Rudo, MSA Capital ETM, Batelco)
- **4 kept nulled** (Daliz, Vizier, MRC, NewReef) — no reliable LinkedIn page found

## Caveats
- `msacap` and `batelco` are parent-company pages, not the specific ADGM licensed entity; downstream enrichment (employee counts, HQ) will reflect the parent, not the ADGM sub.
- Barrenjoey uses the Australian `au.linkedin.com` subdomain.
