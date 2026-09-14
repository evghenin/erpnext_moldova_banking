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
						const msg = r.message || {};
						if (msg.name) {
							frappe.set_route("Form", "Bank Payment Instruction", msg.name);
						}
					},
				});
			}, __("Create"));
		});
	},
});
