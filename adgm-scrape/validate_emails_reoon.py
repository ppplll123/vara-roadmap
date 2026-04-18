"""Validate candidate emails via Reoon Email Verification API.

- 1st pass: validate candidate[0] (firstname.lastname@domain) for every person
- Fallback: if status in {invalid, unknown} AND not catch_all-hit on first, try candidate[1]
- Budget cap: 1000 validations ($1)
- Rate limit: 2s between calls
- Log every 25; checkpoint every 50

Outputs: email-validated.json and email-validation-checkpoint.json
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# Repo root = two parents up from this file (.../vara_roadmap/adgm-scrape/<file>.py)
ROOT = Path(__file__).resolve().parents[2]
CANDIDATES = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'email-candidates.json')
OUT = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'email-validated.json')
CHECKPOINT = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'email-validation-checkpoint.json')
LOG = str(ROOT / 'vara_roadmap' / 'adgm-scrape' / 'email-validation.log')

BUDGET_CAP = 1000
SLEEP = 0.3  # Reoon paid tier allows 60+ req/min; 2.0 was drip-feed

GOOD_STATUSES = {'safe_to_send', 'valid'}
# catch_all is ambiguous — accept if score is high enough
DOMAIN_STATUSES = {'catch_all'}
RETRY_STATUSES = {'invalid', 'unknown', 'disabled'}


def read_key() -> str:
    env = Path('/Users/openclaw/job-auto-apply/.env').read_text()
    for line in env.splitlines():
        if line.startswith('REOON_API_KEY='):
            return line.split('=', 1)[1].strip()
    raise RuntimeError('REOON_API_KEY not found in .env')


def verify(email: str, key: str) -> dict:
    """Call Reoon API. Retries up to 3x on HTTP 429 with a 30s backoff.

    Raises urllib.error.HTTPError / URLError for other transport failures so
    callers can decide how to handle (they used to be swallowed by a bare
    `except Exception` which masked rate-limit errors as silent "successes").
    """
    url = (
        'https://emailverifier.reoon.com/api/v1/verify/'
        f'?email={urllib.parse.quote(email)}&key={key}&mode=power'
    )
    last_err: Exception | None = None
    for attempt in range(3):
        req = urllib.request.Request(url)
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                log(f'  Reoon 429 rate-limit (attempt {attempt+1}/3) — sleeping 30s')
                time.sleep(30)
                continue
            # Other HTTP errors: propagate
            raise
        except urllib.error.URLError as e:
            last_err = e
            # Transient network error — brief backoff, limited retry
            log(f'  Reoon URLError (attempt {attempt+1}/3): {e}')
            time.sleep(5)
            continue
    # Exhausted retries
    assert last_err is not None
    raise last_err


def log(line: str) -> None:
    stamp = time.strftime('%Y-%m-%d %H:%M:%S')
    msg = f'[{stamp}] {line}'
    print(msg, flush=True)
    with open(LOG, 'a') as f:
        f.write(msg + '\n')


def save(path: str, data) -> None:
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)


def load_checkpoint() -> dict:
    if Path(CHECKPOINT).exists():
        with open(CHECKPOINT) as f:
            return json.load(f)
    return {'results': [], 'processed_ids': [], 'calls_used': 0}


def main():
    key = read_key()
    with open(CANDIDATES) as f:
        people = json.load(f)

    # only those with candidates
    people = [p for p in people if p.get('candidates')]
    log(f'Starting validation for {len(people)} people, budget cap {BUDGET_CAP}')

    cp = load_checkpoint()
    results = list(cp['results'])
    processed = set(cp['processed_ids'])
    calls = int(cp['calls_used'])
    log(f'Resuming: {len(processed)} already processed, {calls} calls used')

    for idx, p in enumerate(people):
        pid = p.get('person_id')
        if pid in processed:
            continue
        if calls >= BUDGET_CAP:
            log(f'Budget cap reached ({BUDGET_CAP}), stopping')
            break

        candidates = p['candidates']
        chosen = None
        attempts = []
        # Try up to 2 candidates
        for i, email in enumerate(candidates[:2]):
            if calls >= BUDGET_CAP:
                break
            try:
                resp = verify(email, key)
                calls += 1
            except (urllib.error.HTTPError, urllib.error.URLError) as e:
                # Narrow: log known transport errors, skip to next candidate.
                log(f'  ERROR calling Reoon for {email}: {e}')
                attempts.append({'email': email, 'error': str(e)})
                time.sleep(SLEEP)
                continue
            # Any other unexpected exception (KeyboardInterrupt, JSONDecodeError,
            # programmer errors) is intentionally NOT caught here — it should
            # fail the run loudly rather than silently produce empty results.
            status = resp.get('status')
            score = resp.get('overall_score', 0)
            attempts.append({
                'email': email,
                'status': status,
                'score': score,
                'is_safe_to_send': resp.get('is_safe_to_send'),
                'is_deliverable': resp.get('is_deliverable'),
                'is_catch_all': resp.get('is_catch_all'),
                'can_connect_smtp': resp.get('can_connect_smtp'),
                'is_role_account': resp.get('is_role_account'),
                'is_disposable': resp.get('is_disposable'),
            })
            # Accept if safe_to_send / valid
            if status in GOOD_STATUSES or resp.get('is_safe_to_send'):
                chosen = attempts[-1]
                break
            # Stop on catch_all (domain accepts everything, can't tell per-user)
            if status in DOMAIN_STATUSES:
                chosen = attempts[-1]
                break
            # Retry next candidate only on invalid/unknown
            if status in RETRY_STATUSES and i < len(candidates[:2]) - 1:
                time.sleep(SLEEP)
                continue
            # otherwise break with this attempt as best
            chosen = attempts[-1]
            break
            time.sleep(SLEEP)

        # sleep between people
        time.sleep(SLEEP)

        result = {
            'person_id': pid,
            'name': p.get('name'),
            'firm': p.get('firm'),
            'domain': p.get('domain'),
            'linkedin_url': p.get('linkedin_url'),
            'validated_email': chosen['email'] if chosen else None,
            'validation_status': chosen['status'] if chosen and 'status' in chosen else None,
            'confidence': chosen.get('score') if chosen else None,
            'smtp_check': chosen.get('can_connect_smtp') if chosen else None,
            'is_safe_to_send': chosen.get('is_safe_to_send') if chosen else None,
            'is_deliverable': chosen.get('is_deliverable') if chosen else None,
            'is_catch_all': chosen.get('is_catch_all') if chosen else None,
            'is_role_account': chosen.get('is_role_account') if chosen else None,
            'attempts': attempts,
        }
        results.append(result)
        processed.add(pid)

        if len(processed) % 25 == 0:
            log(f'Progress: {len(processed)}/{len(people)} processed, {calls} Reoon calls')

        if len(processed) % 50 == 0:
            save(CHECKPOINT, {
                'results': results,
                'processed_ids': list(processed),
                'calls_used': calls,
            })
            log(f'Checkpoint saved ({len(results)} results)')

    # Final save
    save(OUT, results)
    save(CHECKPOINT, {
        'results': results,
        'processed_ids': list(processed),
        'calls_used': calls,
    })

    # Summary
    good = [r for r in results if r['validation_status'] in GOOD_STATUSES or r.get('is_safe_to_send')]
    catch_all = [r for r in results if r['validation_status'] == 'catch_all']
    invalid = [r for r in results if r['validation_status'] == 'invalid']
    log('=' * 60)
    log(f'DONE. {len(results)} people processed, {calls} Reoon calls')
    log(f'  safe/valid: {len(good)}')
    log(f'  catch_all:  {len(catch_all)}')
    log(f'  invalid:    {len(invalid)}')
    log(f'  other:      {len(results) - len(good) - len(catch_all) - len(invalid)}')
    log(f'Estimated cost: ${calls * 0.001:.3f}')
    log(f'Wrote: {OUT}')


if __name__ == '__main__':
    main()
