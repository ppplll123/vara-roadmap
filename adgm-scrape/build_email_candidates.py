"""Build email candidates for Brave-found LinkedIn profiles.

Reads:
  - brave-search-results.json (482 linkedin hits)
  - firms-linkedin-enriched.json (primary domain source)
  - master-data.json (fallback domain source)

Writes:
  - email-candidates.json
"""
import json
import re
from pathlib import Path
from urllib.parse import urlparse

# Repo root = two parents up from this file (.../vara_roadmap/adgm-scrape/build_email_candidates.py)
ROOT = Path(__file__).resolve().parents[2]
BRAVE = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'brave-search-results.json')
FIRMS_ENRICHED = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'firms-linkedin-enriched.json')
MASTER = str(ROOT / 'antweave_roadmap' / 'master-data.json')
OUT = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'email-candidates.json')


def norm(s: str) -> str:
    """Normalize a firm name for matching."""
    if not s:
        return ''
    s = s.lower()
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)
    s = re.sub(r'\b(limited|ltd|llc|inc|incorporated|corp|corporation|company|co|plc|capital|holding|holdings|management|partners|international|middle|east|me|ame|mena|uae|ad|ae)\b', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def first_token(s: str) -> str:
    n = norm(s)
    return n.split(' ')[0] if n else ''


def website_to_domain(url: str) -> str | None:
    if not url:
        return None
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return None
    if host.startswith('www.'):
        host = host[4:]
    # Reject social / generic hosts
    bad_hosts = {'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com',
                 'wikipedia.org', 'crunchbase.com', 'bloomberg.com'}
    if host in bad_hosts:
        return None
    if not host or '.' not in host:
        return None
    return host


def slug(s: str) -> str:
    """lowercase alnum-only."""
    return re.sub(r'[^a-z0-9]', '', (s or '').lower())


def build_candidates(first: str, last: str, domain: str) -> list[str]:
    f = slug(first)
    l = slug(last)
    if not f or not l or not domain:
        return []
    return [
        f'{f}.{l}@{domain}',
        f'{f}@{domain}',
        f'{f[0]}.{l}@{domain}',
        f'{f}{l}@{domain}',
    ]


def main():
    with open(BRAVE) as fp:
        brave = json.load(fp)
    with open(FIRMS_ENRICHED) as fp:
        firms_enriched = json.load(fp)
    with open(MASTER) as fp:
        master = json.load(fp)

    # Build firm -> domain map
    # 1. primary: firms-linkedin-enriched (keyed by firm_name)
    primary = {}
    for f in firms_enriched:
        name = f.get('firm_name')
        web = f.get('website')
        d = website_to_domain(web) if web else None
        if name and d:
            primary[name] = d
            primary[norm(name)] = d

    # 2. fallback: master-data firms (keyed by n)
    fallback = {}
    for f in master.get('firms', []):
        name = f.get('n')
        web = f.get('web')
        d = website_to_domain(web) if web else None
        if name and d:
            fallback[name] = d
            fallback[norm(name)] = d

    # Also index by first token for looser matching
    by_token_primary = {}
    for name, d in primary.items():
        tok = first_token(name)
        if tok and len(tok) >= 3:
            by_token_primary.setdefault(tok, d)
    by_token_fallback = {}
    for name, d in fallback.items():
        tok = first_token(name)
        if tok and len(tok) >= 3:
            by_token_fallback.setdefault(tok, d)

    def resolve_domain(firm_name: str, query: str) -> tuple[str | None, str]:
        """Return (domain, source)."""
        if not firm_name:
            return None, 'no_firm'
        # exact
        if firm_name in primary:
            return primary[firm_name], 'primary_exact'
        n = norm(firm_name)
        if n in primary:
            return primary[n], 'primary_norm'
        if firm_name in fallback:
            return fallback[firm_name], 'fallback_exact'
        if n in fallback:
            return fallback[n], 'fallback_norm'

        # Match by first meaningful token
        tok = first_token(firm_name)
        # Prefer token from query second group if present: "First Last" "FirmFirst" linkedin
        if query:
            m = re.findall(r'"([^"]+)"', query)
            if len(m) >= 2:
                query_tok = slug(m[1]).lower()
                if query_tok:
                    # Look up by query token too
                    qt = re.sub(r'[^a-z0-9]', '', query_tok)
                    # Try primary by token
                    for name, d in primary.items():
                        ft = first_token(name)
                        if ft == query_tok or (ft and qt.startswith(ft)):
                            return d, 'primary_query_token'
                    for name, d in fallback.items():
                        ft = first_token(name)
                        if ft == query_tok or (ft and qt.startswith(ft)):
                            return d, 'fallback_query_token'

        if tok and tok in by_token_primary:
            return by_token_primary[tok], 'primary_token'
        if tok and tok in by_token_fallback:
            return by_token_fallback[tok], 'fallback_token'

        # Tight substring match: require >=8 chars of overlap AND same first token
        def tight_match(n_a: str, n_b: str) -> bool:
            if not n_a or not n_b:
                return False
            ta = n_a.split(' ')[0] if n_a else ''
            tb = n_b.split(' ')[0] if n_b else ''
            if not ta or not tb or ta != tb:
                return False
            return len(n_a) >= 6 and len(n_b) >= 6

        for src_name, d in primary.items():
            if isinstance(src_name, str) and tight_match(n, norm(src_name)):
                return d, 'primary_substring'
        for src_name, d in fallback.items():
            if isinstance(src_name, str) and tight_match(n, norm(src_name)):
                return d, 'fallback_substring'

        # Manual overrides for well-known firms that won't match
        overrides = {
            'HSBC Bank Middle East Limited': 'hsbc.com',
            'State Street Bank and Trust Company': 'statestreet.com',
            'TPG (A) Limited': 'tpg.com',
            'Winton Capital Management Limited': 'winton.com',
            'Shorooq Partners Ltd': 'shorooq.com',
            'Ninety One Gulf Capital Limited': 'ninetyone.com',
            'Vision Bank Limited': 'visionbank.com',
            'Blantyre Capital II Limited': 'blantyrecapital.com',
            'Eldridge International Management Limited': 'eldridge.com',
            'Venturesouq Management Limited': 'vsq.com',
            'B&Y Venture Partners Limited': 'bnyventurepartners.com',
            'Polyvalent Capital Limited': 'polyvalent.com',
            'TAQA Insurance Limited': 'taqa.com',
            'ZE Transaction Solutions Ltd': 'ze.com',
            'Hidden Road Partners CIV (Middle East) Limited': 'hiddenroad.com',
            'CROWN AGENTS GLOBAL MARKETS LTD': 'crownagents.com',
            'Finstreet Global Clearing and Settlement Limited': 'finstreet.com',
            'Finstreet Global Markets Limited': 'finstreet.com',
            'Finstreet Capital Limited': 'finstreet.com',
            'Kotak Mahindra Financial Services Limited': 'kotak.com',
            'PraxisIFM Trust Limited': 'praxisifm.com',
            'BlackRock Advisors (UK) Limited': 'blackrock.com',
            'Shuaa GMC Limited': 'shuaa.com',
            'TMF Group Fiduciary Services Limited': 'tmf-group.com',
            'Eiffel Investment ME Limited': 'eiffel-ig.com',
            'Goldman Sachs International': 'gs.com',
            'Hashgraph Ventures Manager Ltd': 'hashgraph.com',
            'Premier Investment Partners Limited': 'premierinvestmentpartners.com',
            'BARING ASSET MANAGEMENT LIMITED': 'barings.com',
            'Salica Ventures Limited': 'salicainvestments.com',
            'Carlyle MENA Advisors Limited': 'carlyle.com',
            'Flat6Labs Arabia Limited': 'flat6labs.com',
            'Kirkoswald Global Management Services (MENA) Ltd': 'kirkoswald.com',
            'OurCrowd Management (Arabia) Limited': 'ourcrowd.com',
            'UBS AG': 'ubs.com',
            'Hashed Global Management Limited': 'hashed.com',
            'Universal Digital INTL Limited': 'universaldigital.com',
            'Lone Star Europe Acquisitions Limited': 'lonestarfunds.com',
            'KIMMERIDGE ENERGY MANAGEMENT COMPANY, LLC': 'kimmeridge.com',
            'UNBOUND MENA LIMITED': 'unbound.com',
            'Pharo Manarah Limited': 'pharomanagement.com',
            'Hafeet Capital Limited': 'hafeetcapital.com',
            'QMM Ltd': 'qmm.com',
            'Zilla Capital for Investments Ltd': 'zillacapital.com',
            'Oyster Re Brokers Limited': 'oysterrebrokers.com',
        }
        if firm_name in overrides:
            return overrides[firm_name], 'override'
        return None, 'no_match'

    out = []
    stats = {'no_match': 0, 'matched': 0, 'source_counts': {}}
    for rec in brave:
        li = rec.get('best_linkedin') or ''
        if not li or 'linkedin.com' not in li:
            continue
        first = rec.get('first_name', '')
        last = rec.get('last_name', '')
        firm = rec.get('firm_name', '')
        q = rec.get('query', '')
        domain, src = resolve_domain(firm, q)
        stats['source_counts'][src] = stats['source_counts'].get(src, 0) + 1
        candidates = build_candidates(first, last, domain) if domain else []
        if domain:
            stats['matched'] += 1
        else:
            stats['no_match'] += 1
        out.append({
            'person_id': rec.get('person_id'),
            'name': f'{first} {last}'.strip(),
            'first_name': first,
            'last_name': last,
            'firm': firm,
            'domain': domain,
            'domain_source': src,
            'linkedin_url': li,
            'candidates': candidates,
        })

    with open(OUT, 'w') as fp:
        json.dump(out, fp, indent=2)

    print(f'Total linkedin records: {len(out)}')
    print(f'With domain: {stats["matched"]}')
    print(f'Without domain: {stats["no_match"]}')
    print('Source breakdown:')
    for s, c in sorted(stats['source_counts'].items(), key=lambda x: -x[1]):
        print(f'  {s}: {c}')
    total_candidates = sum(len(r['candidates']) for r in out)
    print(f'Total candidate emails: {total_candidates}')
    print(f'Wrote: {OUT}')


if __name__ == '__main__':
    main()
