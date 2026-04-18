"""Task B — Reoon-validate legacy emails (people with em set but em_verified unset).

Reads /Users/openclaw/playwright/antweave_roadmap/master-data.json, finds every
person with p.em set AND p.em_verified not true, calls Reoon verifier in
POWER mode for each, writes results to
/Users/openclaw/playwright/vara_roadmap/adgm-scrape/email-validated-legacy.json.

Then updates master-data.json in-place:
  - for every verified person, sets p.em_reoon_status = <status>
  - sets p.em_verified = True only if is_safe_to_send AND status == 'safe'
  - keeps p.em regardless (Option A data)

This is a pure validation + bookkeeping script. The Reoon API (public, paid
endpoint at emailverifier.reoon.com) returns a deliverability verdict.
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

MASTER = Path('/Users/openclaw/playwright/antweave_roadmap/master-data.json')
OUT = Path('/Users/openclaw/playwright/vara_roadmap/adgm-scrape/email-validated-legacy.json')
LOG = Path('/Users/openclaw/playwright/vara_roadmap/adgm-scrape/email-validated-legacy.log')
ENV = Path('/Users/openclaw/job-auto-apply/.env')
CHECKPOINT = Path('/Users/openclaw/playwright/vara_roadmap/adgm-scrape/email-validated-legacy-checkpoint.json')

BUDGET_CAP = 300   # 260 expected + small headroom
SLEEP = 0.35


def read_key() -> str:
    for line in ENV.read_text().splitlines():
        if line.startswith('REOON_API_KEY='):
            return line.split('=', 1)[1].strip()
    raise RuntimeError('REOON_API_KEY missing')


def log(line: str) -> None:
    stamp = time.strftime('%Y-%m-%d %H:%M:%S')
    msg = f'[{stamp}] {line}'
    print(msg, flush=True)
    with LOG.open('a') as f:
        f.write(msg + '\n')


def verify(email: str, key: str) -> dict:
    url = (
        'https://emailverifier.reoon.com/api/v1/verify/'
        f'?email={urllib.parse.quote(email)}&key={key}&mode=power'
    )
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, timeout=45) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            last_err = e
            if e.code == 429:
                log(f'  429 rate-limit attempt={attempt + 1} — sleeping 30s')
                time.sleep(30)
                continue
            raise
        except urllib.error.URLError as e:
            last_err = e
            log(f'  URLError attempt={attempt + 1}: {e}')
            time.sleep(5)
    assert last_err is not None
    raise last_err


def load_checkpoint() -> dict:
    if CHECKPOINT.exists():
        return json.loads(CHECKPOINT.read_text())
    return {'results': [], 'done_pids': [], 'calls': 0}


def save_checkpoint(state):
    CHECKPOINT.write_text(json.dumps(state, indent=2))


def main():
    key = read_key()
    d = json.loads(MASTER.read_text())
    people = d['people']

    targets = [p for p in people if p.get('em') and not p.get('em_verified')]
    log(f'Targets: {len(targets)} unverified emails')

    state = load_checkpoint()
    results = list(state['results'])
    done = set(state['done_pids'])
    calls = int(state['calls'])
    log(f'Resuming: {len(done)} already done, {calls} calls used')

    for idx, p in enumerate(targets):
        pid = p['id']
        if pid in done:
            continue
        if calls >= BUDGET_CAP:
            log(f'Budget cap {BUDGET_CAP} reached')
            break
        em = p['em']
        try:
            resp = verify(em, key)
            calls += 1
        except (urllib.error.HTTPError, urllib.error.URLError) as e:
            log(f'  skip {pid} {em}: {e}')
            results.append({
                'person_id': pid,
                'name': f"{p.get('fn', '')} {p.get('ln', '')}".strip(),
                'firm': p.get('co'),
                'email': em,
                'error': str(e),
            })
            done.add(pid)
            continue

        r = {
            'person_id': pid,
            'name': f"{p.get('fn', '')} {p.get('ln', '')}".strip(),
            'firm': p.get('co'),
            'email': em,
            'status': resp.get('status'),
            'overall_score': resp.get('overall_score'),
            'is_safe_to_send': resp.get('is_safe_to_send'),
            'is_deliverable': resp.get('is_deliverable'),
            'is_catch_all': resp.get('is_catch_all'),
            'is_role_account': resp.get('is_role_account'),
            'is_disposable': resp.get('is_disposable'),
            'can_connect_smtp': resp.get('can_connect_smtp'),
        }
        results.append(r)
        done.add(pid)

        if len(done) % 25 == 0:
            log(f'Progress: {len(done)}/{len(targets)} calls={calls}')
        if len(done) % 50 == 0:
            save_checkpoint({'results': results, 'done_pids': list(done), 'calls': calls})
            log('Checkpoint saved')
        time.sleep(SLEEP)

    save_checkpoint({'results': results, 'done_pids': list(done), 'calls': calls})
    OUT.write_text(json.dumps(results, indent=2))
    log(f'Wrote {OUT}  ({len(results)} results)')

    # ---- write-back to master-data.json ----
    by_pid = {r['person_id']: r for r in results if 'status' in r}
    upd_safe = 0
    upd_any = 0
    for p in people:
        r = by_pid.get(p['id'])
        if not r:
            continue
        status = r.get('status')
        if status is not None:
            p['em_reoon_status'] = status
            upd_any += 1
        if r.get('is_safe_to_send') and status == 'safe':
            p['em_verified'] = True
            upd_safe += 1

    MASTER.write_text(json.dumps(d, indent=2, ensure_ascii=False))
    log(f'master-data.json updated: {upd_any} rows got em_reoon_status; {upd_safe} marked em_verified=true')

    # Summary
    safe = sum(1 for r in results if r.get('status') == 'safe' and r.get('is_safe_to_send'))
    catch_all = sum(1 for r in results if r.get('status') == 'catch_all')
    invalid = sum(1 for r in results if r.get('status') == 'invalid')
    unknown = sum(1 for r in results if r.get('status') == 'unknown')
    errors = sum(1 for r in results if r.get('error'))
    log('=' * 50)
    log(f'{len(results)} processed, {calls} calls, cost ≈ ${calls * 0.001:.3f}')
    log(f'  safe:      {safe}')
    log(f'  catch_all: {catch_all}')
    log(f'  invalid:   {invalid}')
    log(f'  unknown:   {unknown}')
    log(f'  errors:    {errors}')


if __name__ == '__main__':
    main()
