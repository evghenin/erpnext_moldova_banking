// Copyright (c) 2025, Evgheni Nemerenco and contributors
// For license information, please see license.txt

frappe.ui.form.on('Moldova Banking Settings', {
    refresh(frm) {
        set_options_for_idno_selects(frm);
    }
});

function set_options_for_idno_selects(frm) {
    // Company
    frappe.model.with_doctype('Company', () => {
        const fields = frappe.meta.get_docfields('Company');
        const data_fields = fields
            .filter(df => df.fieldtype === 'Data')
            .map(df => df.fieldname);

        frm.set_df_property('company_idno_field', 'options', [''].concat(data_fields));
    });

    // Customer
    frappe.model.with_doctype('Customer', () => {
        const fields = frappe.meta.get_docfields('Customer');
        const data_fields = fields
            .filter(df => df.fieldtype === 'Data')
            .map(df => df.fieldname);

        frm.set_df_property('customer_idno_field', 'options', [''].concat(data_fields));
    });

    // Supplier
    frappe.model.with_doctype('Supplier', () => {
        const fields = frappe.meta.get_docfields('Supplier');
        const data_fields = fields
            .filter(df => df.fieldtype === 'Data')
            .map(df => df.fieldname);

        frm.set_df_property('supplier_idno_field', 'options', [''].concat(data_fields));
    });
}
