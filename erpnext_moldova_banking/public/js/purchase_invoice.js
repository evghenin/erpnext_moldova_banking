function open_unsaved_bank_payment_instruction(data) {
	if (!data) {
		return;
	}
	frappe.model.with_doctype("Bank Payment Instruction", () => {
		const doc = frappe.model.get_new_doc("Bank Payment Instruction");
		const invoices = data.invoices || [];
		delete data.invoices;
		delete data.name;
		delete data.__islocal;
		Object.assign(doc, data);
		doc.__islocal = 1;
		doc.invoices = [];
		for (const row of invoices) {
			const child = frappe.model.add_child(doc, "Bank Payment Instruction Invoice", "invoices");
			Object.assign(child, row);
		}
		frappe.set_route("Form", "Bank Payment Instruction", doc.name);
	});
}

frappe.ui.form.on("Purchase Invoice", {
	refresh(frm) {
		if (frm.doc.docstatus !== 1) {
			return;
		}
		if (flt(frm.doc.outstanding_amount) <= 0) {
			return;
		}
		frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled").then((enabled) => {
			if (!cint(enabled)) {
				return;
			}
			frm.add_custom_button(__("Bank Payment Instruction"), () => {
				frappe.call({
					method: "erpnext_moldova_banking.utils.bank_payment_instruction.make_from_purchase_invoice",
					args: { purchase_invoice: frm.doc.name },
					freeze: true,
					callback(r) {
						open_unsaved_bank_payment_instruction(r.message);
					},
				});
			}, __("Create"));
		});
	},
});
