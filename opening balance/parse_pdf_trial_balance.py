"""
Parse Tally PDF Trial Balance -> JSON
Handles hierarchical indentation levels, Debit/Credit columns, carry-forward rows.
"""

import pdfplumber
import json
import re

PDF_PATH = r'new_TrialBal.pdf'
OUT_PATH = r'pdf_trial_balance.json'

# X positions (from inspection):
# Debit values appear at x1 ~ 420-445  (right edge near ~445)
# Credit values appear at x1 ~ 502-570 (right edge near ~570)
# Name texts: x0 determines indent level
DEBIT_X_MIN = 390
DEBIT_X_MAX = 480
CREDIT_X_MIN = 480
CREDIT_X_MAX = 600

SKIP_TEXTS = {
    'Opening Balance', 'Debit', 'Credit',
    'Carried Over', 'Brought Forward', 'continued ...',
    'Trial Balance', 'Page', 'For 1-Apr-25', 'For',
}

def is_header_or_skip(text):
    t = text.strip()
    for s in SKIP_TEXTS:
        if s in t:
            return True
    if re.match(r'^Trial Balance.*Page \d+$', t):
        return True
    if re.match(r'^For \d', t):
        return True
    return False

def x0_to_level(x0):
    """Map x0 position to hierarchy level (0=top, 1, 2, 3)"""
    if x0 < 50:
        return 0
    elif x0 < 70:
        return 1
    elif x0 < 90:
        return 2
    else:
        return 3

def parse_amount(text):
    """Parse Indian number format -> float"""
    t = text.strip().replace(',', '')
    try:
        return float(t)
    except ValueError:
        return None

def parse_pdf(pdf_path):
    rows = []  # list of {name, level, debit, credit}
    company = ''
    period = ''

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages):
            words = page.extract_words(keep_blank_chars=True, x_tolerance=3, y_tolerance=3)

            # Extract company name and period from first page
            if page_num == 0:
                for w in words[:5]:
                    if 'PVT LTD' in w['text'] or 'LTD' in w['text'] or 'INDIA' in w['text']:
                        company = w['text']
                    elif 'For ' in w['text']:
                        period = w['text']

            # Group words by row (same top value ±2px)
            line_groups = {}
            for w in words:
                top = round(w['top'] / 3) * 3  # snap to 3px grid
                if top not in line_groups:
                    line_groups[top] = []
                line_groups[top].append(w)

            for top in sorted(line_groups.keys()):
                line_words = line_groups[top]

                # Separate into name words, debit words, credit words by x position
                name_words = []
                debit_words = []
                credit_words = []

                for w in line_words:
                    x0 = w['x0']
                    x1 = w['x1']
                    # The debit column center is around x=430, credit around x=530
                    if x0 >= DEBIT_X_MIN and x1 <= DEBIT_X_MAX + 5:
                        debit_words.append(w)
                    elif x0 >= CREDIT_X_MIN:
                        credit_words.append(w)
                    else:
                        name_words.append(w)

                if not name_words:
                    continue

                name_text = ' '.join(w['text'] for w in name_words).strip()

                # Skip page headers/footers
                if is_header_or_skip(name_text):
                    continue
                if not name_text:
                    continue

                # Skip company name line
                if name_text == company:
                    continue

                # Resolve indent level from x0 of first name word
                level = x0_to_level(name_words[0]['x0'])

                debit = None
                credit = None
                if debit_words:
                    amt = parse_amount(' '.join(w['text'] for w in debit_words))
                    if amt is not None:
                        debit = amt
                if credit_words:
                    amt = parse_amount(' '.join(w['text'] for w in credit_words))
                    if amt is not None:
                        credit = amt

                rows.append({
                    'name': name_text,
                    'level': level,
                    'debit': debit,
                    'credit': credit,
                })

    return company, period, rows

def build_tree(rows):
    """Convert flat rows with levels to nested tree structure"""
    def make_node(row):
        return {
            'name': row['name'],
            'level': row['level'],
            'debit': row['debit'],
            'credit': row['credit'],
            'children': []
        }

    root = {'name': 'ROOT', 'level': -1, 'debit': None, 'credit': None, 'children': []}
    stack = [root]

    for row in rows:
        node = make_node(row)
        # Find parent: walk back stack to find node with lower level
        while len(stack) > 1 and stack[-1]['level'] >= node['level']:
            stack.pop()
        stack[-1]['children'].append(node)
        stack.append(node)

    return root['children']

def flatten_for_display(rows):
    """Return flat list optimized for table display with indent level"""
    result = []
    for r in rows:
        result.append({
            'name': r['name'],
            'level': r['level'],
            'debit': r['debit'],
            'credit': r['credit'],
        })
    return result

def main():
    print(f"Parsing {PDF_PATH}...")
    company, period, rows = parse_pdf(PDF_PATH)

    print(f"Company: {company}")
    print(f"Period: {period}")
    print(f"Total rows extracted: {len(rows)}")

    # Calculate totals
    total_debit = sum(r['debit'] for r in rows if r['debit'] and r['level'] == 0)
    total_credit = sum(r['credit'] for r in rows if r['credit'] and r['level'] == 0)

    # Build output
    output = {
        'company': company,
        'period': period,
        'total_debit': total_debit,
        'total_credit': total_credit,
        'rows': rows,
    }

    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"Written to {OUT_PATH}")
    print(f"Sample rows:")
    for r in rows[:10]:
        indent = '  ' * r['level']
        dr = f"DR:{r['debit']:,.2f}" if r['debit'] else ''
        cr = f"CR:{r['credit']:,.2f}" if r['credit'] else ''
        print(f"  {indent}{r['name']}  {dr}  {cr}")

if __name__ == '__main__':
    main()
