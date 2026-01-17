// ERPNext v15 – Moldova Banking Settings (FINAL FIX)
// Child table behavior for automation_rules:
// - `account` and `cost_center` is disabled until `company` is selected
// - `account` and `cost_center` are cleared whenever `company` is changed or cleared
// - `account` and `cost_center` queries are filtered by selected `company`
//
// NOTE: The child DocType name may vary depending on your setup.
// We bind handlers to BOTH names to ensure the logic always runs:
// - "Moldova Banking Automation Rule"
// - "Bank Transaction Automation Rule"

frappe.ui.form.on('Moldova Banking Settings', {
    onload(frm) {
        set_automation_mop_filter(frm);
    },
    refresh(frm) {
        set_automation_mop_filter(frm);
        apply_automation_rules_logic(frm);
        set_options_for_idno_selects(frm);
    },
    regenerate_bnm_rates_key(frm) {
        const btn = frm.get_field("regenerate_bnm_rates_key")?.$input;
        if (btn) btn.prop("disabled", true);

        frappe.call({
        method: "erpnext_moldova_banking.utils.bnm_key.regenerate_bnm_rates_key",
        freeze: true,
        freeze_message: __("Generating new key..."),
        })
        .then(() => frm.reload_doc())
        .finally(() => {
            if (btn) btn.prop("disabled", false);
        });
    },

    configure_currency_exchange_bnm(frm) {
        const btn = frm.get_field("configure_currency_exchange_bnm")?.$input;
        if (btn) btn.prop("disabled", true);

        frappe.call({
        method: "erpnext_moldova_banking.utils.bnm_key.configure_currency_exchange_bnm",
        freeze: true,
        freeze_message: __("Configuring..."),
        })
        .then(() => frappe.show_alert({ message: __("Configured."), indicator: "green" }))
        .finally(() => {
            if (btn) btn.prop("disabled", false);
        });
    },
});

function set_automation_mop_filter(frm) {
    frm.set_query('automation_pe_mode_of_payment', function () {
        return { filters: { type: 'Bank' } };
    });
}

function apply_automation_rules_logic(frm) {
    const table = frm.fields_dict.automation_rules;
    if (!table || !table.grid) return;

    const grid = table.grid;

    // Filter account by selected company
    grid.get_field('account').get_query = function (doc, cdt, cdn) {
        const row = locals[cdt][cdn];
        if (!row || !row.company) {
            return { filters: { name: '__invalid__' } };
        }
        return {
            filters: {
                company: row.company,
                is_group: 0
            }
        };
    };

    // Filter cost_center by selected company
    grid.get_field('cost_center').get_query = function (doc, cdt, cdn) {
        const row = locals[cdt][cdn];
        if (!row || !row.company) {
            return { filters: { name: '__invalid__' } };
        }
        return {
            filters: {
                company: row.company
            }
        };
    };
}

// Shared handlers for child rows
function on_rule_form_render(frm, cdt, cdn) {
    toggle_account_field(frm, cdt, cdn);
    toggle_cost_center_field(frm, cdt, cdn);
}

function on_rule_company_change(frm, cdt, cdn) {
    const row = locals[cdt][cdn];
    if (!row) return;

    // Requirement: if company changes OR is cleared -> account and cost_center must be cleared
    if (row.account) {
        frappe.model.set_value(cdt, cdn, 'account', null);
    }
    if (row.cost_center) {
        frappe.model.set_value(cdt, cdn, 'cost_center', null);
    }

    toggle_account_field(frm, cdt, cdn);
    toggle_cost_center_field(frm, cdt, cdn);
}

function toggle_account_field(frm, cdt, cdn) {
    const table = frm.fields_dict.automation_rules;
    if (!table || !table.grid) return;

    const grid_row = table.grid.grid_rows_by_docname[cdn];
    if (!grid_row) return;

    const row = locals[cdt][cdn];
    const enable = !!(row && row.company);

    // Disable until company is selected
    grid_row.toggle_editable('account', enable);
}

function toggle_cost_center_field(frm, cdt, cdn) {
    const table = frm.fields_dict.automation_rules;
    if (!table || !table.grid) return;

    const grid_row = table.grid.grid_rows_by_docname[cdn];
    if (!grid_row) return;

    const row = locals[cdt][cdn];
    const enable = !!(row && row.company);

    // Disable until company is selected
    grid_row.toggle_editable('cost_center', enable);
}

// Bind to both possible child doctypes to avoid silent non-execution
['Moldova Banking Automation Rule', 'Bank Transaction Automation Rule'].forEach((child_doctype) => {
    frappe.ui.form.on(child_doctype, {
        form_render(frm, cdt, cdn) {
            on_rule_form_render(frm, cdt, cdn);
        },
        company(frm, cdt, cdn) {
            on_rule_company_change(frm, cdt, cdn);
        }
    });
});

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
