import re

# 1. Update database schema for getting expense ledgers
with open("database_manager.py", "r", encoding="utf-8") as f:
    db_content = f.read()

if "def get_expense_ledgers(" not in db_content:
    expense_q = """
def get_expense_ledgers():
    conn = get_db_connection()
    rows = conn.execute("SELECT name FROM ledgers WHERE LOWER(type) = 'other' OR LOWER(type) = 'others'").fetchall()
    conn.close()
    return [r['name'].strip().lower() for r in rows if r['name']]
"""
    db_content += "\n" + expense_q
    with open("database_manager.py", "w", encoding="utf-8") as f:
        f.write(db_content)
    print("Updated database_manager.py")

# 2. Update frontend UI templates
with open('templates/payments_made.html', 'r', encoding='utf-8') as f:
    html = f.read()

if 'id="paymentsTabs"' not in html:
    tabs_html = """
        <!-- SPLIT ACTIONS & TABS -->
        <div class="mb-4">
            <button onclick="splitExpenses()" id="splitBtn" class="btn btn-warning mb-3">
                <i class="bi bi-diagram-2 me-2"></i> Split Expenses
            </button>
            <ul class="nav nav-tabs" id="paymentsTabs" role="tablist">
                <li class="nav-item" role="presentation">
                    <button class="nav-link active fw-bold" id="vendor-tab" data-bs-toggle="tab" data-bs-target="#vendor-pane" type="button" role="tab" onclick="activeTab='vendor'; renderPayments()">Vendor Payments (<span id="vendorCount">0</span>)</button>
                </li>
                <li class="nav-item" role="presentation">
                    <button class="nav-link fw-bold text-warning" id="expense-tab" data-bs-toggle="tab" data-bs-target="#expense-pane" type="button" role="tab" onclick="activeTab='expense'; renderPayments()">Expenses (<span id="expenseCount">0</span>)</button>
                </li>
            </ul>
        </div>
        <!-- KPI CARDS -->"""
        
    html = html.replace('<!-- KPI CARDS -->', tabs_html)

if "async function splitExpenses()" not in html:
    js_vars = """
        let allPayments = [];
        let vendorPayments = [];
        let expensePayments = [];
        let activeTab = 'vendor';
        let totalAmount = 0;
    """
    html = re.sub(r'let allPayments = \[\];\s*let totalAmount = 0;', js_vars, html)
    
    html = re.sub(r'renderPayments\(allPayments\);', 'updateTabCounts(); renderPayments();', html)
    html = re.sub(r'renderPayments\(filtered\);', 'renderPayments(filtered);', html)
    
    js = """
        async function splitExpenses() {
            if (allPayments.length === 0) {
                alert("No payments available to split. Please fetch data first.");
                return;
            }
            
            const btn = document.getElementById('splitBtn');
            const originalText = btn.innerHTML;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Splitting...';
            btn.disabled = true;

            try {
                const res = await fetch('/api/payments/split_expenses', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({ payments: allPayments })
                });
                const data = await res.json();
                
                if (data.status === 'success') {
                    vendorPayments = data.vendor_payments;
                    expensePayments = data.expenses;
                    
                    updateTabCounts();
                    renderPayments();
                    alert(data.message);
                } else {
                    alert('Error: ' + data.error);
                }
            } catch (err) {
                console.error(err);
                alert('Connection Error');
            } finally {
                btn.innerHTML = originalText;
                btn.disabled = false;
            }
        }
        
        function updateTabCounts() {
            if (vendorPayments.length === 0 && expensePayments.length === 0) {
                // Initial load, haven't split yet
                document.getElementById('vendorCount').innerText = allPayments.length;
                document.getElementById('expenseCount').innerText = '0';
            } else {
                document.getElementById('vendorCount').innerText = vendorPayments.length;
                document.getElementById('expenseCount').innerText = expensePayments.length;
            }
        }
    """
    
    render_func_replacement = """
        function renderPayments(customData = null) {
            let dataToRender = customData;
            
            if (!customData) {
                if (vendorPayments.length > 0 || expensePayments.length > 0) {
                    dataToRender = activeTab === 'vendor' ? vendorPayments : expensePayments;
                } else {
                    if (activeTab === 'expense') {
                        dataToRender = []; // nothing split yet
                    } else {
                        dataToRender = allPayments;
                    }
                }
            }
            
            const tbody = document.getElementById('tableBody');
            
            const count = dataToRender.length;
            const amt = dataToRender.reduce((s, p) => s + (parseFloat(p.amount) || 0), 0);
            document.getElementById('total-payments').innerText = count;
            document.getElementById('total-amount').innerText = '₹' + amt.toLocaleString();

            if (!dataToRender || dataToRender.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" class="text-center py-4 text-muted">No records found for this view</td></tr>';
                return;
            }

            tbody.innerHTML = dataToRender.map((p, idx) => {
                const realIdx = allPayments.indexOf(p);
                const clickIdx = realIdx !== -1 ? realIdx : idx;
                
                return `
                <tr style="cursor: pointer;" onclick="showPaymentDetails(${clickIdx})">
                    <td class="fw-medium text-danger">${p.payment_number}</td>
                    <td>${formatDate(p.date)}</td>
                    <td>${p.vendor_name}</td>
                    <td>
                        <span class="badge ${p.payment_mode === 'Cash' ? 'bg-success' : 'bg-primary'}">
                            ${p.payment_mode || 'Bank'}
                        </span>
                    </td>
                    <td class="text-end fw-bold">₹${parseFloat(p.amount).toLocaleString()}</td>
                    <td class="text-center">
                        <button class="btn btn-sm btn-outline-danger" onclick="event.stopPropagation(); showPaymentDetails(${clickIdx})">
                            <i class="bi bi-eye"></i> View
                        </button>
                    </td>
                </tr>`;
            }).join('');
        }
    """
    
    # We replace the old `function renderPayments(payments) { ... }` block entirely.
    # It might be tricky with regex if it spans many lines, wait.
    match = re.search(r'function renderPayments\(payments\)\s*\{.*?\}\s*(?=function updateKPIs)', html, flags=re.DOTALL)
    if match:
        html = html.replace(match.group(0), render_func_replacement + '\n\n')
    else:
        # Let's dynamically replace the tbody innerHTML block.
        match = re.search(r'function renderPayments\(payments\)\s*\{.*?(?=function showPaymentDetails)', html, flags=re.DOTALL)
        if match:
            html = html.replace(match.group(0), render_func_replacement + '\n\n        ')

    pos = html.rfind('</script>')
    if pos != -1:
        html = html[:pos] + js + '\n    ' + html[pos:]
            
    with open('templates/payments_made.html', 'w', encoding='utf-8') as f:
        f.write(html)
    print("Updated templates/payments_made.html")
