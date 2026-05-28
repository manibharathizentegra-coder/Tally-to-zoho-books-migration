import os

HTML_FILES = [
    "invoices.html",
    "bills.html",
    "receipts.html",
    "sales_orders.html",
    "purchase_orders.html",
    "journals.html"
]

for html_file in HTML_FILES:
    filepath = os.path.join("templates", html_file)
    if not os.path.exists(filepath):
        continue
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    if 'id="jsonUpload"' not in content:
        # It's already there at the bottom script, but the button wasn't added because it couldn't find exportToJson
        # So we search for fetchData button
        
        target = '<button onclick="fetchData()" class="btn btn-primary d-flex align-items-center">'
        if target in content:
            upload_html = """
                <input type="file" id="jsonUpload" accept=".json" style="display: none;" onchange="uploadJson(event)">
                <button onclick="document.getElementById('jsonUpload').click()" class="btn btn-outline-success d-flex align-items-center">
                    <i class="bi bi-upload me-2"></i> Upload JSON
                </button>
"""
            content = content.replace(target, upload_html + target)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f" Injected button into {html_file}")
        else:
            print(f" Could not find fetchData button in {html_file}")
    else:
        # Wait, the previous script might have added id="jsonUpload" in the `<script>` block string!
        # Let's count occurrences
        if content.count('id="jsonUpload"') < 2 and 'type="file"' not in content:
            target = '<button onclick="fetchData()" class="btn btn-primary d-flex align-items-center">'
            if target in content:
                upload_html = """
                <input type="file" id="jsonUpload" accept=".json" style="display: none;" onchange="uploadJson(event)">
                <button onclick="document.getElementById('jsonUpload').click()" class="btn btn-outline-success d-flex align-items-center">
                    <i class="bi bi-upload me-2"></i> Upload JSON
                </button>
"""
                content = content.replace(target, upload_html + target)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f" Injected button into {html_file}")
