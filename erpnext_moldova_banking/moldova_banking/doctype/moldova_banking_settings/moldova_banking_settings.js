// Copyright (c) 2025, Evgheni Nemerenco
// ERPNext v15 – Moldova Banking Settings
//
// Fixes:
// - Proper filtering of clearing_account by company in child table
// - clearing_account disabled when company is not selected
// - clearing_account cleared when company changes to another one

frappe.ui.form.on('Moldova Banking Settings', {
    onload(frm) {
        set_pos_clearing_mop_filter(frm);
        apply_pos_clearing_rules_logic(frm);
    },
    refresh(frm) {
        set_pos_clearing_mop_filter(frm);
        apply_pos_clearing_rules_logic(frm);
        set_options_for_idno_selects(frm);
    }
});

function set_pos_clearing_mop_filter(frm) {
    frm.set_query('pos_clearing_mode_of_payment', function () {
        return {
            filters: {
                type: 'Bank'
            }
        };
    });
}

function apply_pos_clearing_rules_logic(frm) {
    // Correct way: override get_query of the grid field
    const grid = frm.fields_dict.pos_clearing_rules.grid;

    grid.get_field('clearing_account').get_query = function (doc, cdt, cdn) {
        const row = locals[cdt][cdn];

        if (!row.company) {
            return {
                filters: { name: '__invalid__' }
            };
        }

        return {
            filters: {
                company: row.company,
                is_group: 0
            }
        };
    };
}

frappe.ui.form.on('Moldova Banking POS Clearing Rule', {
    company(frm, cdt, cdn) {
        const row = locals[cdt][cdn];

        // Clear account if company is removed
        if (!row.company && row.clearing_account) {
            frappe.model.set_value(cdt, cdn, 'clearing_account', null);
        }

        // Clear account if it belongs to another company
        if (row.company && row.clearing_account) {
            frappe.db.get_value('Account', row.clearing_account, 'company')
                .then(r => {
                    if (r?.message?.company !== row.company) {
                        frappe.model.set_value(cdt, cdn, 'clearing_account', null);
                    }
                });
        }

        toggle_clearing_account_field(frm, cdt, cdn);
    },

    form_render(frm, cdt, cdn) {
        toggle_clearing_account_field(frm, cdt, cdn);
    }
});

function toggle_clearing_account_field(frm, cdt, cdn) {
    const grid = frm.fields_dict.pos_clearing_rules.grid;
    const grid_row = grid.grid_rows_by_docname[cdn];

    if (!grid_row) return;

    grid_row.toggle_editable('clearing_account', !!locals[cdt][cdn].company);
}

// Existing logic preserved
function set_options_for_idno_selects(frm) {
    frappe.model.with_doctype('Company', () => {
        const fields = frappe.meta.get_docfields('Company');
        const data_fields = fields.filter(df => df.fieldtype === 'Data').map(df => df.fieldname);
        frm.set_df_property('company_idno_field', 'options', [''].concat(data_fields));
    });

    frappe.model.with_doctype('Customer', () => {
        const fields = frappe.meta.get_docfields('Customer');
        const data_fields = fields.filter(df => df.fieldtype === 'Data').map(df => df.fieldname);
        frm.set_df_property('customer_idno_field', 'options', [''].concat(data_fields));
    });

    frappe.model.with_doctype('Supplier', () => {
        const fields = frappe.meta.get_docfields('Supplier');
        const data_fields = fields.filter(df => df.fieldtype === 'Data').map(df => df.fieldname);
        frm.set_df_property('supplier_idno_field', 'options', [''].concat(data_fields));
    });
}