import os
import glob
import re

template_dir = 'templates'
files = glob.glob(os.path.join(template_dir, '*.html'))

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if Contra is already in there
    if 'href="/contra"' in content:
        continue
    
    # We find the Payments Made li and inject Contra right after it
    if 'href="/payments_made"' in content:
        # Find the closing </li> of payments_made
        idx = content.find('href="/payments_made"')
        if idx != -1:
            end_li = content.find('</li>', idx)
            if end_li != -1:
                insert_pos = end_li + 5
                
                insertion = '''
                <li class="nav-item me-3">
                    <a class="nav-link" href="/contra"><i class="bi bi-arrow-left-right me-1"></i> Contra</a>
                </li>'''
                
                new_content = content[:insert_pos] + insertion + content[insert_pos:]
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(new_content)
                print(f'Updated navbar in {filepath}')
