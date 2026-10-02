frappe.listview_settings["Purchase Invoice"] = {
	onload(listview) {
		frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled").then((enabled) => {
			if (!cint(enabled)) {
				return;
			}
			listview.page.add_action_item(__("Bank Payment Instruction"), () => {
				const names = listview.get_checked_items(true);
				if (!names.length) {
					frappe.msgprint(__("Select at least one Purchase Invoice."));
					return;
				}
				frappe.call({
					method: "erpnext_moldova_banking.utils.bank_payment_instruction.validate_purchase_invoices_for_instruction",
					args: { purchase_invoices: names },
					freeze: true,
					callback(r) {
						const check = r.message || {};
						if (!check.ok) {
							frappe.msgprint({
								title: __("Cannot create Bank Payment Instruction"),
								indicator: "red",
								message: (check.errors || []).join("<br>"),
							});
							return;
						}
						frappe.call({
							method: "erpnext_moldova_banking.utils.bank_payment_instruction.make_from_purchase_invoices",
							args: { purchase_invoices: names },
							freeze: true,
							callback(create) {
								const data = create.message;
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
										const child = frappe.model.add_child(
											doc,
											"Bank Payment Instruction Invoice",
											"invoices"
										);
										Object.assign(child, row);
									}
									frappe.set_route("Form", "Bank Payment Instruction", doc.name);
								});
							},
						});
					},
				});
			});
		});
	},
};
