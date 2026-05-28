"""
Patch items.html:
1. Accept CSV + XLSX for file input
2. Replace <select> with <input list="datalist"> for searchable DB fields
3. Keep fmUpdateMapping compatible with input events
"""
import os, re

path = os.path.join(os.path.dirname(__file__), 'templates', 'items.html')
with open(path, 'r', encoding='utf-8') as f:
    content = f.read()

# ── Patch 1: Accept CSV too ──────────────────────────────────────────
OLD_ACCEPT = 'id="fmZohoFieldsInput" accept=".xlsx"'
NEW_ACCEPT = 'id="fmZohoFieldsInput" accept=".xlsx,.csv"'
if OLD_ACCEPT in content:
    content = content.replace(OLD_ACCEPT, NEW_ACCEPT, 1)
    print("Patch 1 OK: accept attribute updated")
else:
    print("Patch 1 FAIL")

# ── Patch 2: Replace "Import Zoho Fields" button label hint ──────────
OLD_TITLE = 'title="Upload Zoho Books sample XLSX to import field names"'
NEW_TITLE = 'title="Upload Zoho Books sample XLSX or CSV to import field names"'
if OLD_TITLE in content:
    content = content.replace(OLD_TITLE, NEW_TITLE, 1)
    print("Patch 2 OK: button title updated")
else:
    print("Patch 2 FAIL")

# ── Patch 3: Replace fmRenderTable to use input+datalist ─────────────
OLD_RENDER = """      function fmRenderTable() {
        const tbody = document.getElementById('fmTableBody');
        if (!fmZohoFields.length) {
          tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted py-4">No Zoho fields loaded. Upload a Zoho XLSX sample first.</td></tr>';
          return;
        }
        tbody.innerHTML = fmZohoFields.map(function(zf) {
          const sel = fmMapping[zf] || '';
          const opts = fmDbFields.map(function(f) {
            return '<option value="' + f + '"' + (f===sel?' selected':'') + '>' + f + '</option>';
          }).join('');
          return '<tr><td class="ps-3 fw-medium" style="color:#2563eb">' + zf + '</td>'
            + '<td><select class="form-select form-select-sm" data-zoho-field="' + zf + '" onchange="fmUpdateMapping(this)" style="font-size:12px">'
            + '<option value="">-- Skip --</option>' + opts + '</select></td></tr>';
        }).join('');
        fmRefreshCount();
      }

      function fmUpdateMapping(sel) {
        const zf = sel.getAttribute('data-zoho-field');
        if (sel.value) fmMapping[zf] = sel.value;
        else delete fmMapping[zf];
        fmRefreshCount();
      }"""

NEW_RENDER = """      function fmRenderTable() {
        const tbody = document.getElementById('fmTableBody');
        if (!fmZohoFields.length) {
          tbody.innerHTML = '<tr><td colspan="2" class="text-center text-muted py-4">No Zoho fields loaded. Upload a Zoho XLSX or CSV sample first.</td></tr>';
          return;
        }

        // Build shared datalist of DB fields
        const dlId = 'fmDbFieldsList';
        let dl = document.getElementById(dlId);
        if (!dl) {
          dl = document.createElement('datalist');
          dl.id = dlId;
          document.body.appendChild(dl);
        }
        dl.innerHTML = fmDbFields.map(function(f) { return '<option value="' + f + '">'; }).join('');

        tbody.innerHTML = fmZohoFields.map(function(zf) {
          const cur = fmMapping[zf] || '';
          return '<tr>'
            + '<td class="ps-3 fw-medium" style="color:#2563eb">' + zf + '</td>'
            + '<td class="pe-2 py-1">'
            + '<div class="input-group input-group-sm">'
            + '<input type="text" list="' + dlId + '" class="form-control form-control-sm fmDbInput"'
            + ' data-zoho-field="' + zf + '"'
            + ' value="' + cur + '"'
            + ' placeholder="Search or type DB field..."'
            + ' autocomplete="off"'
            + ' oninput="fmUpdateMapping(this)"'
            + ' style="font-size:12px">'
            + '<button type="button" class="btn btn-outline-secondary btn-sm px-2" title="Clear" onclick="fmClearField(this)" style="font-size:11px">'
            + '<i class=\\'bi bi-x\\'></i>'
            + '</button>'
            + '</div>'
            + '</td>'
            + '</tr>';
        }).join('');
        fmRefreshCount();
      }

      function fmUpdateMapping(inp) {
        const zf = inp.getAttribute('data-zoho-field');
        const val = inp.value.trim();
        // Only save if it's a valid DB field (or empty to skip)
        if (val && fmDbFields.includes(val)) {
          fmMapping[zf] = val;
          inp.style.borderColor = '';
          inp.style.background = '#f0fdf4'; // light green = valid
        } else if (!val) {
          delete fmMapping[zf];
          inp.style.background = '';
          inp.style.borderColor = '';
        } else {
          // typed but not yet a valid match — keep pending, highlight yellow
          inp.style.background = '#fefce8';
          inp.style.borderColor = '#ca8a04';
        }
        fmRefreshCount();
      }

      function fmClearField(btn) {
        const inp = btn.closest('.input-group').querySelector('input');
        inp.value = '';
        fmUpdateMapping(inp);
        inp.focus();
      }"""

if OLD_RENDER in content:
    content = content.replace(OLD_RENDER, NEW_RENDER, 1)
    print("Patch 3 OK: fmRenderTable replaced with searchable datalist inputs")
else:
    print("Patch 3 FAIL: could not find fmRenderTable")
    idx = content.find("fmRenderTable")
    print(repr(content[idx:idx+300]))

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Done.")
