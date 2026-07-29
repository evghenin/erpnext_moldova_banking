frappe.ui.form.on("Payment Order", {
	refresh(frm) {
		frm.trigger("toggle_maib_buttons");
	},

	toggle_maib_buttons(frm) {
		if (frm.doc.docstatus !== 1) {
			return;
		}

		frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled").then((enabled) => {
			if (!cint(enabled)) {
				return;
			}

			const status = frm.doc.maib_status || "Not Sent";
			const can_send =
				!frm.doc.maib_instruction_id ||
				["Not Sent", "API Error", "Rejected"].includes(status);

			if (can_send) {
				frm.add_custom_button(
					__("Send to MAIB"),
					() => {
						frm.trigger("send_to_maib");
					},
					__("MAIB")
				);
			}

			if (frm.doc.maib_instruction_id) {
				frm.add_custom_button(
					__("Refresh Status"),
					() => {
						frm.trigger("refresh_maib_status");
					},
					__("MAIB")
				);
			}

			if (status === "Executed" && !frm.doc.maib_payment_entry) {
				frm.add_custom_button(
					__("Match & Create Payment Entry"),
					() => {
						frm.trigger("match_create_payment_entry");
					},
					__("MAIB")
				);
			}
		});
	},

	send_to_maib(frm) {
		if ((frm.doc.references || []).length !== 1) {
			frappe.msgprint({
				title: __("MAIB Payment"),
				message: __("MAIB requires exactly one payment reference on the Payment Order."),
				indicator: "orange",
			});
			return;
		}

		frappe.confirm(__("Send this Payment Order to MAIB?"), () => {
			frappe.call({
				method: "erpnext_moldova_banking.utils.maib_payment_order.send_payment_order_to_maib",
				args: { name: frm.doc.name },
				freeze: true,
				freeze_message: __("Sending to MAIB..."),
				callback(r) {
					frm.reload_doc();
					if (r.message) {
						frappe.show_alert({
							message: __("MAIB status: {0}", [r.message.maib_status || ""]),
							indicator: "green",
						});
					}
				},
			});
		});
	},

	refresh_maib_status(frm) {
		frappe.call({
			method: "erpnext_moldova_banking.utils.maib_payment_order.refresh_payment_order_maib_status",
			args: { name: frm.doc.name },
			freeze: true,
			freeze_message: __("Refreshing MAIB status..."),
			callback(r) {
				frm.reload_doc();
				if (r.message) {
					frappe.show_alert({
						message: __("MAIB status: {0}", [r.message.maib_status || ""]),
						indicator: "blue",
					});
				}
			},
		});
	},

	match_create_payment_entry(frm) {
		frappe.confirm(
			__(
				"Find matching Bank Transaction and create Payment Entry for this Payment Order?"
			),
			() => {
				frappe.call({
					method:
						"erpnext_moldova_banking.utils.maib_payment_match.match_payment_order_to_bank_transaction",
					args: { name: frm.doc.name },
					freeze: true,
					freeze_message: __("Matching and creating Payment Entry..."),
					callback(r) {
						frm.reload_doc();
						const msg = r.message || {};
						if (msg.ok) {
							frappe.show_alert({
								message: __(
									"Payment Entry {0} created / linked (Bank Transaction {1})",
									[msg.payment_entry || "", msg.bank_transaction || ""]
								),
								indicator: "green",
							});
						} else if (msg.skipped === "no_match") {
							frappe.msgprint({
								title: __("No Match"),
								message: __(
									"No matching submitted Bank Transaction was found for this Payment Order."
								),
								indicator: "orange",
							});
						}
					},
				});
			}
		);
	},
});
