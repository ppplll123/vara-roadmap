"""Smartlead campaign expansion.

Pulls the current state of master-data.json (after Task B + Task C), computes
per-cluster eligible leads (people whose firm is in cluster X AND email is
safe per Reoon), fetches existing leads per Smartlead campaign, and uploads
the *new* ones (not already present).

For Cluster 2 (VARA-crypto broker-dealer), if no campaign exists yet and
there are eligible leads, create it (DRAFT) and upload.

Every campaign stays in DRAFTED state — nothing is auto-started.
"""
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

MASTER = Path('/Users/openclaw/playwright/antweave_roadmap/master-data.json')
OUT = Path('/Users/openclaw/playwright/vara_roadmap/smartlead-expansion-result.json')

CLUSTER_CAMPAIGNS = {
    0: {'id': 3196899, 'name': 'ICP-0-Boutique-Advisory-Undisclosed-HQ'},
    1: {'id': 3196900, 'name': 'ICP-1-Global-Advisory-US-HQ'},
    3: {'id': 3196901, 'name': 'ICP-3-Mid-Cap-Advisory-FLAGGED-prior-reg-action'},
    4: {'id': 3196902, 'name': 'ICP-4-Large-Advisory-UAE-HQ'},
    # Cluster 2 campaign will be created on demand if there are eligible leads.
}

CLUSTER_2_NAME = 'ICP-2-Large-Broker-Dealer-VARA-crypto'

# Sequences for cluster 2 (reused from smartlead_setup.py logic)
C2_SEQ = [
    {
        'subject': "{{first_name}}, VARA Cat 1 issuance and {{company_name}}",
        'body': (
            "<p>Hi {{first_name}},<br><br>"
            "{{company_name}} sits in a VARA-licensed broker-dealer cohort. "
            "Only two firms hold the VA Issuance (Cat 1) permission needed to "
            "issue or distribute tokenised products into ADGM — Ctrl Alt Solutions DMCC "
            "being one. Everyone else routes through them.<br><br>"
            "I have a one-pager on the Cat 1 topology if helpful.<br><br>"
            "Peter Lewinski, PhD (Oxford Law &amp; Finance)</p>"
        ),
        'delay': 0,
    },
    {
        'subject': "Re: the Cat 1 topology — one-pager for {{company_name}}",
        'body': (
            "<p>{{first_name}},<br><br>"
            "Quick follow-up with the one-pager: (1) the two Cat 1 VA Issuance "
            "licensees today, (2) three structural alternatives if you need to "
            "issue without them, (3) the ADGM FSP interface most broker-dealers miss.<br><br>"
            "Peter</p>"
        ),
        'delay': 3,
    },
    {
        'subject': "{{first_name}}, 20 min on VARA Cat 1?",
        'body': (
            "<p>{{first_name}},<br><br>"
            "Final ping. If Cat 1 is live at {{company_name}}, I have three 20-minute "
            "slots this week to walk the one-pager end-to-end.<br><br>"
            "Calendar: https://cal.com/peter-lewinski/20-min<br><br>"
            "Peter Lewinski, PhD</p>"
        ),
        'delay': 4,
    },
]


def sl(args: list, input_json: Any = None, timeout: int = 120) -> dict:
    cmd = ['smartlead', '--format', 'json'] + args
    tmppath = None
    if input_json is not None:
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as f:
            json.dump(input_json, f)
            tmppath = f.name
        cmd += ['--from-json', tmppath]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            return {
                '_error': f'exit {r.returncode}',
                '_stderr': r.stderr[:500],
                '_stdout': r.stdout[:500],
            }
        try:
            return json.loads(r.stdout) if r.stdout.strip() else {}
        except json.JSONDecodeError:
            return {'_error': 'non-json', '_stdout': r.stdout[:500]}
    finally:
        if tmppath:
            try:
                os.unlink(tmppath)
            except OSError:
                pass


def list_campaign_emails(cid: int) -> set[str]:
    """Page through all leads in a campaign."""
    emails: set[str] = set()
    offset = 0
    while True:
        resp = sl(['leads', 'list', '--campaign-id', str(cid), '--limit', '100', '--offset', str(offset)])
        if isinstance(resp, dict) and resp.get('_error'):
            print(f'  leads list error cid={cid} offset={offset}: {resp}')
            break
        data = resp.get('data', []) if isinstance(resp, dict) else resp
        if not data:
            break
        for row in data:
            lead = row.get('lead') if isinstance(row, dict) else None
            if lead and lead.get('email'):
                emails.add(lead['email'].lower().strip())
        if len(data) < 100:
            break
        offset += 100
        time.sleep(0.2)
    return emails


def build_lead_obj(p: dict, firm: dict) -> dict:
    return {
        'email': p['em'],
        'first_name': p.get('fn') or '',
        'last_name': p.get('ln') or '',
        'company_name': firm['n'],
        'linkedin_profile': p.get('url') or '',
        'location': p.get('loc') or firm.get('hq') or '',
        'custom_fields': {
            'person_id': p['id'],
            'firm_id': firm['id'],
            'role': p.get('r') or '',
            'cluster_id': str(firm.get('cluster_id', '')),
            'cluster_name': firm.get('cluster_name') or '',
            'em_reoon_status': p.get('em_reoon_status') or '',
            'em_verified': 'true' if p.get('em_verified') else 'false',
            'source_register': p.get('src') or '',
        },
    }


def upload_leads(campaign_id: int, leads: list[dict]) -> dict:
    if not leads:
        return {'added_count': 0, 'skipped_count': 0, '_note': 'nothing-to-upload'}
    payload = {
        'lead_list': leads,
        'settings': {
            'ignore_global_block_list': False,
            'ignore_unsubscribe_list': False,
            'ignore_duplicate_leads_in_other_campaign': False,
            'ignore_community_bounce_list': False,
        },
    }
    return sl(['leads', 'add', '--campaign-id', str(campaign_id)], input_json=payload)


def create_c2_campaign() -> Optional[int]:
    resp = sl(['campaigns', 'create', '--name', CLUSTER_2_NAME])
    if resp.get('_error') or 'id' not in resp:
        print(f'  create C2 failed: {resp}')
        return None
    cid = resp['id']
    print(f'  created C2 campaign id={cid}')
    # Save sequence
    seq_body = {
        'sequences': [
            {
                'id': None,
                'seq_number': i + 1,
                'subject': e['subject'],
                'email_body': e['body'],
                'seq_delay_details': {'delay_in_days': e['delay']},
            }
            for i, e in enumerate(C2_SEQ)
        ]
    }
    seq = sl(['campaigns', 'save-sequence', '--id', str(cid)], input_json=seq_body)
    if seq.get('_error'):
        print(f'  [warn] C2 sequence save error: {seq}')
    else:
        print('  C2 sequence saved (3 emails)')
    return cid


def main():
    d = json.loads(MASTER.read_text())
    people = d['people']
    firms_by_name = {f['n']: f for f in d['firms']}

    # Build per-cluster pool of "safe" leads (em_verified=true)
    pool: dict[int, list[dict]] = {0: [], 1: [], 2: [], 3: [], 4: []}
    for p in people:
        if not p.get('em') or not p.get('em_verified'):
            continue
        firm = firms_by_name.get(p.get('co'))
        if not firm:
            continue
        cid = firm.get('cluster_id')
        if cid is None or cid not in pool:
            continue
        pool[cid].append(build_lead_obj(p, firm))

    print('Eligible (safe) leads per cluster:')
    for k, v in pool.items():
        print(f'  cluster {k}: {len(v)}')

    report: dict = {'generated_at': time.strftime('%Y-%m-%dT%H:%M:%S'), 'campaigns': []}

    # --- handle the 4 existing campaigns ---
    for cid, info in CLUSTER_CAMPAIGNS.items():
        camp_id = info['id']
        print(f'\nCluster {cid} -> campaign {camp_id} ({info["name"]})')
        existing = list_campaign_emails(camp_id)
        print(f'  existing leads: {len(existing)}')
        new = [ld for ld in pool[cid] if ld['email'].lower().strip() not in existing]
        print(f'  new eligible:   {len(new)}')
        upload_resp = upload_leads(camp_id, new)
        print(f'  upload: {upload_resp}')
        report['campaigns'].append({
            'cluster_id': cid,
            'campaign_id': camp_id,
            'name': info['name'],
            'existing_leads': len(existing),
            'new_uploaded_candidates': len(new),
            'upload_response': upload_resp,
            'final_total_expected': len(existing) + len(new),
        })
        time.sleep(0.5)

    # --- cluster 2 ---
    c2 = pool[2]
    if c2:
        print(f'\nCluster 2: {len(c2)} eligible leads — creating campaign {CLUSTER_2_NAME}')
        c2_id = create_c2_campaign()
        if c2_id:
            upload_resp = upload_leads(c2_id, c2)
            print(f'  C2 upload: {upload_resp}')
            report['campaigns'].append({
                'cluster_id': 2,
                'campaign_id': c2_id,
                'name': CLUSTER_2_NAME,
                'existing_leads': 0,
                'new_uploaded_candidates': len(c2),
                'upload_response': upload_resp,
                'final_total_expected': len(c2),
            })
    else:
        print('\nCluster 2: 0 eligible leads — skipping campaign creation')
        report['campaigns'].append({
            'cluster_id': 2,
            'campaign_id': None,
            'name': CLUSTER_2_NAME,
            'existing_leads': 0,
            'new_uploaded_candidates': 0,
            'upload_response': None,
            'final_total_expected': 0,
            'note': 'No verified-safe emails in cluster 2 — campaign not created',
        })

    OUT.write_text(json.dumps(report, indent=2))
    print(f'\nReport: {OUT}')
    print('\n--- Summary ---')
    for c in report['campaigns']:
        print(
            f"  cluster {c['cluster_id']}: cid={c['campaign_id']} "
            f"existing={c['existing_leads']} added={c['new_uploaded_candidates']} "
            f"expected_total={c['final_total_expected']}"
        )


if __name__ == '__main__':
    main()
