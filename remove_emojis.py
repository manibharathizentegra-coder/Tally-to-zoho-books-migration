import os
import re

def remove_emojis_from_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Regex to match emojis (simple ranges)
        emoji_pattern = re.compile(
            "[\U00010000-\U0010ffff]"
            "|[\u2600-\u27FF]"
            "|[\u2B50\u2139]"
        )
        
        new_content = emoji_pattern.sub('', content)
        if new_content != content:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Removed emojis from {filepath}")
    except Exception as e:
        pass

for root, dirs, files in os.walk('.'):
    for file in files:
        if file.endswith('.py'):
            filepath = os.path.join(root, file)
            remove_emojis_from_file(filepath)
