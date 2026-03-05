"""
Export reconciliation results as JSON for the frontend dashboard.
Standalone script - does not import reconcile_pdf_vs_zoho.py
"""
import json, re
import pdfplumber
import openpyxl
import warnings
from datetime import datetime

PDF_PATH   = 'new_TrialBal.pdf'
EXCEL_PATH = 'zoho_report.xlsx'
OUT_PATH   = 'comparison_result.json'
MATCH_THRESHOLD = 0.60

SKIP_TEXTS = {'Opening Balance', 'Debit', 'Credit', 'Carried Over',
              'Brought Forward', 'continued ...', 'Trial Balance', 'Page', 'For 1-Apr-25'}
DEBIT_X_MIN, DEBIT_X_MAX, CREDIT_X_MIN = 390, 480, 480

def is_skip(text):
    t = text.strip()
    return any(s in t for s in SKIP_TEXTS) or bool(re.match(r'^(Trial Balance.*Page \d+|For \d)', t))

def x0_to_level(x0):
    if x0 < 50: return 0
    if x0 < 70: return 1
    if x0 < 90: return 2
    return 3

def parse_amount(text):
    try: return float(text.strip().replace(',', ''))
    except: return None

def parse_tally_pdf():
    rows = []; company = ''; period = ''
    with pdfplumber.open(PDF_PATH) as pdf:
        for pn, page in enumerate(pdf.pages):
            words = page.extract_words(keep_blank_chars=True, x_tolerance=3, y_tolerance=3)
            if pn == 0:
                for w in words[:5]:
                    if any(k in w['text'] for k in ['PVT LTD', 'LTD', 'INDIA']): company = w['text']
                    elif 'For ' in w['text']: period = w['text']
            groups = {}
            for w in words:
                top = round(w['top'] / 3) * 3
                groups.setdefault(top, []).append(w)
            for top in sorted(groups):
                lw = groups[top]
                nw, dw, cw = [], [], []
                for w in lw:
                    if w['x0'] >= DEBIT_X_MIN and w['x1'] <= DEBIT_X_MAX+5: dw.append(w)
                    elif w['x0'] >= CREDIT_X_MIN: cw.append(w)
                    else: nw.append(w)
                if not nw: continue
                name = ' '.join(w['text'] for w in nw).strip()
                if is_skip(name) or not name or name == company: continue
                debit  = parse_amount(' '.join(w['text'] for w in dw)) if dw else None
                credit = parse_amount(' '.join(w['text'] for w in cw)) if cw else None
                rows.append({'name': name, 'level': x0_to_level(nw[0]['x0']),
                             'debit': debit or 0.0, 'credit': credit or 0.0})
    return company, period, rows

def parse_zoho_excel():
    rows = []; company = ''; period = ''
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        wb = openpyxl.load_workbook(EXCEL_PATH)
    ws = wb.active
    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            txt = str(row[0] or '')
            lines = [l.strip() for l in txt.split('\n') if l.strip()]
            if lines: company = lines[0]
            for l in lines:
                if 'As of' in l or 'as of' in l: period = l
            continue
        if i == 1: continue
        if row[0] is None and row[1] is None: continue
        name = str(row[0]).strip() if row[0] else ''
        if not name or name in ('Account ', 'Total for Trial Balance', ''): continue
        indent = len(str(row[0])) - len(name)
        clean = name.strip()
        if clean.lower().startswith('total for '): continue
        try:
            debit  = float(row[1]) if row[1] not in (None, '') else 0.0
            credit = float(row[2]) if row[2] not in (None, '') else 0.0
        except: debit = credit = 0.0
        rows.append({'name': clean, 'level': indent // 10, 'debit': debit, 'credit': credit})
    return company, period, rows

def normalise(name):
    n = re.sub(r'[^a-z0-9 ]', ' ', name.lower())
    n = re.sub(r'\s+', ' ', n).strip()
    return n.replace(' a/c', ' account').replace(' &', ' and')

def similarity(a, b):
    ta = set(normalise(a).split()); tb = set(normalise(b).split())
    if not ta or not tb: return 0.0
    return len(ta & tb) / max(len(ta), len(tb))

def reconcile(tally_rows, zoho_rows):
    zoho_lookup = {normalise(r['name']): r for r in zoho_rows}
    matched = []; mismatched = []; only_tally = []; zoho_used = set()

    for tr in tally_rows:
        tkey = normalise(tr['name'])
        if tkey in zoho_lookup:
            zr = zoho_lookup[tkey]; zoho_used.add(tkey)
        else:
            best_score, best_key = 0.0, None
            for zkey in zoho_lookup:
                if zkey in zoho_used: continue
                s = similarity(tr['name'], zoho_lookup[zkey]['name'])
                if s > best_score: best_score, best_key = s, zkey
            if best_score >= MATCH_THRESHOLD and best_key:
                zr = zoho_lookup[best_key]; zoho_used.add(best_key)
            else:
                only_tally.append(tr); continue

        t_net = tr['debit'] - tr['credit']
        z_net = zr['debit'] - zr['credit']
        diff  = abs(t_net - z_net)
        res   = dict(tally_name=tr['name'], zoho_name=zr['name'],
                     tally_dr=tr['debit'], tally_cr=tr['credit'],
                     zoho_dr=zr['debit'], zoho_cr=zr['credit'],
                     t_net=t_net, z_net=z_net, diff=round(diff, 2))
        if diff < 1.0:
            matched.append(res)
        else:
            if abs(abs(t_net) - abs(z_net)) < 1.0: res['root_cause'] = 'DR/CR Side Reversed'
            elif zr['debit'] == 0 and zr['credit'] == 0: res['root_cause'] = 'Zero in Zoho - Opening balance not set'
            else: res['root_cause'] = 'Partial amount mismatch'
            mismatched.append(res)

    only_zoho = [r for k, r in zoho_lookup.items()
                 if k not in zoho_used and (r['debit'] != 0 or r['credit'] != 0)]
    return matched, mismatched, only_tally, only_zoho

def main():
    print('Parsing Tally PDF...')
    t_company, t_period, tally_rows = parse_tally_pdf()
    print(f'  {len(tally_rows)} rows from Tally PDF')

    print('Parsing Zoho Excel...')
    z_company, z_period, zoho_rows = parse_zoho_excel()
    print(f'  {len(zoho_rows)} rows from Zoho Excel')

    print('Reconciling...')
    matched, mismatched, only_tally, only_zoho = reconcile(tally_rows, zoho_rows)
    total = len(matched) + len(mismatched) + len(only_tally) + len(only_zoho)

    output = {
        'generated': datetime.now().isoformat(),
        'tally': {'company': t_company, 'period': t_period, 'total_rows': len(tally_rows)},
        'zoho':  {'company': z_company, 'period': z_period,  'total_rows': len(zoho_rows)},
        'summary': {
            'total': total, 'matched': len(matched),
            'mismatched': len(mismatched), 'only_tally': len(only_tally),
            'only_zoho': len(only_zoho),
            'match_pct': round(len(matched)/total*100, 1) if total else 0,
        },
        'mismatched': [
            {'ledger': r['tally_name'], 'zoho_name': r['zoho_name'],
             'tally_dr': r['tally_dr'], 'tally_cr': r['tally_cr'],
             'zoho_dr':  r['zoho_dr'],  'zoho_cr':  r['zoho_cr'],
             't_net': r['t_net'], 'z_net': r['z_net'], 'diff': r['diff'],
             'root_cause': r['root_cause']}
            for r in mismatched
        ],
        'matched': [
            {'ledger': r['tally_name'], 'zoho_name': r['zoho_name'],
             'tally_dr': r['tally_dr'], 'tally_cr': r['tally_cr'],
             'zoho_dr':  r['zoho_dr'],  'zoho_cr':  r['zoho_cr'], 'diff': 0}
            for r in matched
        ],
        'only_tally': [
            {'ledger': r['name'], 'tally_dr': r['debit'], 'tally_cr': r['credit']}
            for r in only_tally
        ],
        'only_zoho': [
            {'ledger': r['name'], 'zoho_dr': r['debit'], 'zoho_cr': r['credit']}
            for r in only_zoho
        ],
    }

    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f'Written: {OUT_PATH}')
    print(f'  Matched:    {len(matched)}  ({output["summary"]["match_pct"]}%)')
    print(f'  Mismatched: {len(mismatched)}')
    print(f'  Tally only: {len(only_tally)}')
    print(f'  Zoho only:  {len(only_zoho)}')

if __name__ == '__main__':
    main()
