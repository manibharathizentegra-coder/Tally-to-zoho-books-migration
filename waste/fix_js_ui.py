import re

html_path = 'templates/payments_made.html'

with open(html_path, 'r', encoding='utf-8') as f:
    html = f.read()

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
                <tr style="cursor: pointer;" onclick="showDetails(${clickIdx})">
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
                        <button class="btn btn-sm btn-outline-danger" onclick="event.stopPropagation(); showDetails(${clickIdx})">
                            <i class="bi bi-eye"></i> View
                        </button>
                    </td>
                </tr>`;
            }).join('');
        }
"""

# Find `function renderPayments(payments)` 
start_idx = html.find('function renderPayments(payments) {')
if start_idx != -1:
    end_idx = html.find('        function showDetails(idx) {', start_idx)
    if end_idx != -1:
        # replace out the function
        html = html[:start_idx] + render_func_replacement + "\n" + html[end_idx:]

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(html)
print("Fixed JS inside payments_made.html")
