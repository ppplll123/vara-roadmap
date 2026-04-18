"""Task C — Fix orphan firm-name matches.

For every person with `em` set but `co` not matching a firm.n (or missing), try
to fuzzy-match their `co` onto a canonical firm name using:
  1. normalized-equality (strip suffixes / lowercase / alnum only)
  2. Levenshtein distance <= 3 on normalized form
  3. distinctive keyword overlap (>=2 tokens of length >=4)

If a match is found, update p.co to the canonical firm.n (write back to
master-data.json) and log the rename. Write an `orphan-recovery.md` report.

This is a pure data-cleanup script. It does not call any network API.
"""
import json
import re
from collections import Counter
from pathlib import Path

MASTER = Path('/Users/openclaw/playwright/antweave_roadmap/master-data.json')
REPORT = Path('/Users/openclaw/playwright/vara_roadmap/adgm-scrape/orphan-recovery.md')

# Narrow list of ONLY legal-entity suffixes / geographic qualifiers —
# deliberately avoids real name parts like "global", "capital", "partners".
SUFFIX_TOKENS = [
    'limited', 'ltd', 'llc', 'fze', 'fzco', 'dmcc', 'pjsc', 'plc', 'pvt',
    'inc', 'incorporated', 'corporation', 'corp',
    'gmbh', 'nv', 'bv', 'spa', 'srl',
    'me', 'mena', 'middleeast', 'middle', 'east',
    'uae',
    'llp', 'sarl', 'sprl', 'pte',
    'technologies', 'technology',  # very common appendage
    'solutions',
]

STOPWORDS = set(SUFFIX_TOKENS) | {
    'the', 'and', 'of', 'for', 'a', 'an', '&',
}


def norm(s: str) -> str:
    s = (s or '').lower()
    # strip parenthesised segments
    s = re.sub(r'\([^)]*\)', ' ', s)
    # strip punctuation
    s = re.sub(r'[^a-z0-9 ]+', ' ', s)
    # tokenize, drop suffix tokens & stopwords
    toks = [t for t in s.split() if t and t not in STOPWORDS]
    return ' '.join(toks).strip()


def norm_compact(s: str) -> str:
    # alnum-only compact form (no spaces) for equality / Levenshtein
    return re.sub(r'[^a-z0-9]+', '', (s or '').lower())


def norm_compact_stripped(s: str) -> str:
    # compact form AFTER removing suffix tokens (so "hextrustltd" -> "hextrust")
    toks = norm(s).split()
    return ''.join(toks)


def tokens(s: str) -> list[str]:
    # Include length-3 tokens too (e.g. "alt", "ice") — stopword-filtered
    return [t for t in norm(s).split() if len(t) >= 3]


def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            ins = cur[j - 1] + 1
            dele = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            cur.append(min(ins, dele, sub))
        prev = cur
    return prev[-1]


def build_firm_index(firms):
    # Precompute norm variants per firm
    idx = []
    for f in firms:
        n = f['n']
        nc = norm_compact_stripped(n)
        ncfull = norm_compact(n)
        toks = set(tokens(n))
        idx.append({
            'firm': f,
            'name': n,
            'nc_stripped': nc,
            'nc_full': ncfull,
            'tokens': toks,
        })
    return idx


def find_match(co: str, em: str, idx):
    """Return (firm_name, method, detail) or (None, None, None)."""
    co_nc = norm_compact_stripped(co)
    co_ncfull = norm_compact(co)
    co_toks = set(tokens(co))

    # 1. Normalized equality
    for f in idx:
        if co_nc and (co_nc == f['nc_stripped'] or co_ncfull == f['nc_full']):
            return f['name'], 'exact-normalized', ''

    # 2. Substring containment (one inside the other), min-len 5 to avoid junk
    if len(co_nc) >= 5:
        for f in idx:
            if co_nc and f['nc_stripped'] and (
                co_nc in f['nc_stripped'] or f['nc_stripped'] in co_nc
            ):
                # require that the shorter is at least 70% of the longer
                short, lng = sorted([co_nc, f['nc_stripped']], key=len)
                if len(lng) and len(short) / len(lng) >= 0.7:
                    return f['name'], 'substring', f'{co_nc} ⊂ {f["nc_stripped"]}'

    # 3. Levenshtein <= 3 on compact-stripped form, len >= 5
    if len(co_nc) >= 5:
        best = None
        best_d = 4
        for f in idx:
            if not f['nc_stripped'] or abs(len(f['nc_stripped']) - len(co_nc)) > 3:
                continue
            d = levenshtein(co_nc, f['nc_stripped'])
            if d < best_d:
                best_d = d
                best = f
        if best and best_d <= 3:
            return best['name'], 'levenshtein', f'd={best_d}'

    # 4. Distinctive keyword overlap (>=2 tokens of length >=4)
    if len(co_toks) >= 1:
        for f in idx:
            overlap = co_toks & f['tokens']
            if len(overlap) >= 2:
                return f['name'], 'token-overlap', ','.join(sorted(overlap))
            # If co has a single strong token that is fully present
            if len(co_toks) == 1 and co_toks.issubset(f['tokens']):
                tok = next(iter(co_toks))
                if len(tok) >= 6:
                    return f['name'], 'single-token', tok

    # 5. Email-domain heuristic — match firm whose web URL domain
    # matches the email domain's main label.
    if em and '@' in em:
        dom = em.split('@', 1)[1].lower()
        # primary label e.g. "hextrust.com" -> "hextrust"
        label = dom.split('.')[0]
        label_full = dom.rsplit('.', 1)[0].replace('.', '')
        for f in idx:
            web = (f['firm'].get('web') or '').lower()
            if label and label in web and len(label) >= 5:
                return f['name'], 'email-domain', f'{label}@{web}'
            if f['nc_stripped'] and label in f['nc_stripped'] and len(label) >= 5:
                return f['name'], 'email-domain-name', f'{label}→{f["nc_stripped"]}'
            # also try the firm's compact name contained in label
            if f['nc_stripped'] and len(f['nc_stripped']) >= 5 and f['nc_stripped'] in label_full:
                return f['name'], 'email-domain-name', f'{f["nc_stripped"]}⊂{label_full}'

    return None, None, None


def main():
    d = json.loads(MASTER.read_text())
    people = d['people']
    firms = d['firms']
    firm_names = {f['n'] for f in firms}

    idx = build_firm_index(firms)

    # Orphan = has em; co either missing OR not in firm_names
    with_em = [p for p in people if p.get('em')]
    matched_already = [p for p in with_em if p.get('co') in firm_names]
    orphans = [p for p in with_em if p.get('co') not in firm_names]  # includes co=None/"" case

    print(f'with_em={len(with_em)} matched_already={len(matched_already)} orphans={len(orphans)}')

    recoveries = []
    unmatched = []

    for p in orphans:
        co = p.get('co') or ''
        em = p.get('em') or ''
        new_firm, method, detail = find_match(co, em, idx)
        if new_firm:
            recoveries.append({
                'pid': p['id'],
                'name': f"{p.get('fn','')} {p.get('ln','')}".strip(),
                'old_co': co,
                'new_co': new_firm,
                'method': method,
                'detail': detail,
                'em': em,
            })
            # write back
            p['co'] = new_firm
        else:
            unmatched.append({
                'pid': p['id'],
                'name': f"{p.get('fn','')} {p.get('ln','')}".strip(),
                'old_co': co,
                'em': em,
            })

    # Group recoveries by (old_co -> new_co) for summary
    mapping_counter = Counter(
        (r['old_co'], r['new_co'], r['method']) for r in recoveries
    )

    # Write report
    lines = []
    lines.append('# Orphan Firm-Name Recovery Report')
    lines.append('')
    lines.append(f'- Total people with `em`: **{len(with_em)}**')
    lines.append(f'- Already cleanly matched: **{len(matched_already)}**')
    lines.append(f'- Orphans (em set, co not in firms): **{len(orphans)}**')
    lines.append(f'- Recovered: **{len(recoveries)}**')
    lines.append(f'- Still unmatched: **{len(unmatched)}**')
    lines.append('')
    lines.append('## Summary — old_co → new_co (method, count)')
    lines.append('')
    lines.append('| Old co | → | Canonical firm (new co) | Method | Count |')
    lines.append('|---|---|---|---|---|')
    for (old, new, method), cnt in mapping_counter.most_common():
        lines.append(f'| {old!r} | → | {new!r} | {method} | {cnt} |')
    lines.append('')
    lines.append('## Detailed recoveries')
    lines.append('')
    lines.append('| Person ID | Name | old_co | new_co | method | detail | email |')
    lines.append('|---|---|---|---|---|---|---|')
    for r in recoveries:
        lines.append(
            f"| {r['pid']} | {r['name']} | {r['old_co']!r} | {r['new_co']!r} "
            f"| {r['method']} | {r['detail']} | {r['em']} |"
        )
    lines.append('')
    lines.append('## Unmatched (manual review)')
    lines.append('')
    lines.append('| Person ID | Name | co | email |')
    lines.append('|---|---|---|---|')
    for u in unmatched:
        lines.append(f"| {u['pid']} | {u['name']} | {u['old_co']!r} | {u['em']} |")
    REPORT.write_text('\n'.join(lines))
    print(f'Report: {REPORT}')

    # Save master-data.json back
    MASTER.write_text(json.dumps(d, indent=2, ensure_ascii=False))
    print(f'master-data.json updated: {len(recoveries)} p.co renames written')

    # short stdout summary
    print('\nTop recoveries:')
    for (old, new, method), cnt in mapping_counter.most_common(20):
        print(f'  {cnt:3d}  {old!r} → {new!r}  [{method}]')
    print(f'\nUnmatched: {len(unmatched)}')
    for u in unmatched[:20]:
        print(f'  {u["pid"]} co={u["old_co"]!r} em={u["em"]}')


if __name__ == '__main__':
    main()
