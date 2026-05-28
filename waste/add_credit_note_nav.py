import os
import glob
import re

template_dir = 'templates'
files = glob.glob(os.path.join(template_dir, '*.html'))

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if Credit Note is already in there
    if 'href="/credit_note"' in content:
        continue
    
    # We find the Contra li and inject Credit Note right after it
    if 'href="/contra"' in content:
        # Find the closing </li> of contra
        idx = content.find('href="/contra"')
        if idx != -1:
            end_li = content.find('</li>', idx)
            if end_li != -1:
                insert_pos = end_li + 5
                
                insertion = '''
                <li class="nav-item me-3">
                    <a class="nav-link" href="/credit_note"><i class="bi bi-file-earmark-minus me-1"></i> Credit Note</a>
                </li>'''
                
                new_content = content[:insert_pos] + insertion + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f'Updated navbar in {filepath}')
