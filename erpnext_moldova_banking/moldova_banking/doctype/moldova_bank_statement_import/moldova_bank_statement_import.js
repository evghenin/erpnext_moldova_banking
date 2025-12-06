frappe.ui.form.on('Moldova Bank Statement Import', {
    onload(frm) {
        set_bank_account_query(frm);
    },

    refresh(frm) {
        set_bank_account_query(frm);
    },

    company(frm) {
        // Reset values on company change
        frm.set_value('bank_account', null);
        frm.set_value('statement_currency', null);
        set_bank_account_query(frm);
    },

    bank_account(frm) {
        update_statement_currency_from_bank_account(frm);
    }
});


function set_bank_account_query(frm) {
    frm.set_query('bank_account', function() {
        const filters = {};

        if (frm.doc.company) {
            filters['company'] = frm.doc.company;
        } else {
            filters['name'] = '__none__';
        }

        return { filters };
    });
}


function update_statement_currency_from_bank_account(frm) {
    if (!frm.doc.bank_account) {
        frm.set_value('statement_currency', null);
        return;
    }

    // Step 1: Get linked GL account
    frappe.db.get_value(
        'Bank Account',
        frm.doc.bank_account,
        ['account'],
        function (r) {
            if (!r || !r.account) {
                frm.set_value('statement_currency', null);
                return;
            }

            // Step 2: Get currency from GL account
            frappe.db.get_value(
                'Account',
                r.account,
                ['account_currency'],
                function (acc) {
                    if (!acc) {
                        frm.set_value('statement_currency', null);
                        return;
                    }

                    frm.set_value('statement_currency', acc.account_currency || null);
                }
            );
        }
    );
}
