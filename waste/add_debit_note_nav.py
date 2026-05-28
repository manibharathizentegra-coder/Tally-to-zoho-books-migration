import os
import glob
import re

template_dir = 'templates'
files = glob.glob(os.path.join(template_dir, '*.html'))

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if Debit Note is already in there
    if 'href="/debit_note"' in content:
        continue
    
    # We find the Credit Note li and inject Debit Note right after it
    if 'href="/credit_note"' in content:
        # Find the closing </li> of credit note
        idx = content.find('href="/credit_note"')
        if idx != -1:
            end_li = content.find('</li>', idx)
            if end_li != -1:
                insert_pos = end_li + 5
                
                insertion = '''
                <li class="nav-item me-3">
                    <a class="nav-link" href="/debit_note"><i class="bi bi-file-earmark-plus me-1"></i> Debit Note</a>
                </li>'''
                
                new_content = content[:insert_pos] + insertion + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f'Updated navbar in {filepath}')
