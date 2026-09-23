frappe.ui.form.on("Bank Payment Instruction", {
	setup(frm) {
		frm.set_query("party_bank_account", () => party_bank_account_filters(frm));
		frm.set_query("company_bank_account", () => ({
			filters: {
				is_company_account: 1,
				company: frm.doc.company || "",
			},
		}));
		frm.set_query("purchase_invoice", "invoices", () => invoice_filters(frm));
		keep_beneficiary_fields_visible(frm);
	},

	refresh(frm) {
		keep_beneficiary_fields_visible(frm);
		toggle_manual_payment(frm);
		frm.trigger("toggle_bank_buttons");
	},

	company(frm) {
		if (frm.doc.company_bank_account) {
			frappe.db.get_value("Bank Account", frm.doc.company_bank_account, "company").then((r) => {
				if ((r.message || {}).company !== frm.doc.company) {
					frm.set_value("company_bank_account", "").then(() => set_company_bank_account(frm));
				}
			});
		} else {
			set_company_bank_account(frm);
		}
	},

	party_type(frm) {
		frm.set_value("party", "");
		frm.set_value("party_bank_account", "");
		clear_beneficiary(frm);
		toggle_manual_payment(frm, true);
		set_company_bank_account(frm);
	},

	party(frm) {
		frm.set_value("party_bank_account", "");
		frm.trigger("fill_beneficiary");
		set_company_bank_account(frm);
	},

	party_bank_account(frm) {
		frm.trigger("fill_beneficiary");
	},

	fill_beneficiary(frm) {
		if (!frm.doc.party_type || !frm.doc.party) {
			clear_beneficiary(frm);
			return;
		}
		frappe.call({
			method: "erpnext_moldova_banking.utils.bank_payment_instruction.get_beneficiary_defaults",
			args: {
				party_type: frm.doc.party_type,
				party: frm.doc.party,
				party_bank_account: frm.doc.party_bank_account,
			},
			callback(r) {
				const values = r.message || {};
				frm.set_value("beneficiary_name", values.beneficiary_name || "");
				frm.set_value("beneficiary_fiscal_code", values.beneficiary_fiscal_code || "");
				frm.set_value("destination_iban", values.destination_iban || "");
				frm.set_value("destination_bic", values.destination_bic || "");
				frm.set_value("residency_status", values.residency_status || "N");
			},
		});
	},

	toggle_bank_buttons(frm) {
		if (frm.doc.docstatus !== 1) {
			return;
		}
		if (frm.doc.bank_provider !== "MAIB") {
			return;
		}

		frappe.db.get_single_value("Moldova Banking Settings", "maib_outward_payments_enabled").then((enabled) => {
			if (!cint(enabled)) {
				return;
			}

			const status = frm.doc.status || "Not Sent";
			const can_send =
				!frm.doc.bank_instruction_id || ["Not Sent", "API Error", "Rejected"].includes(status);

			if (can_send) {
				frm.add_custom_button(__("Send to Bank"), () => frm.trigger("send_to_bank"), __("MAIB"));
			}
			if (frm.doc.bank_instruction_id) {
				frm.add_custom_button(__("Refresh Status"), () => frm.trigger("refresh_status"), __("MAIB"));
			}
			if (status === "Executed" && !frm.doc.payment_entry) {
				frm.add_custom_button(
					__("Match & Create Payment Entry"),
					() => frm.trigger("match_create_payment_entry"),
					__("MAIB")
				);
			}
		});
	},

	send_to_bank(frm) {
		frappe.confirm(__("Send this Bank Payment Instruction to MAIB?"), () => {
			frappe.call({
				method: "erpnext_moldova_banking.utils.bank_payment_instruction.send_instruction_to_maib",
				args: { name: frm.doc.name },
				freeze: true,
				freeze_message: __("Sending to MAIB..."),
				callback(r) {
					frm.reload_doc();
					if (r.message) {
						frappe.show_alert({
							message: __("Bank status: {0}", [r.message.status || ""]),
							indicator: "green",
						});
					}
				},
			});
		});
	},

	refresh_status(frm) {
		frappe.call({
			method: "erpnext_moldova_banking.utils.bank_payment_instruction.refresh_instruction_status",
			args: { name: frm.doc.name },
			freeze: true,
			freeze_message: __("Refreshing status..."),
			callback(r) {
				frm.reload_doc();
				if (r.message) {
					frappe.show_alert({
						message: __("Bank status: {0}", [r.message.status || ""]),
						indicator: "blue",
					});
				}
			},
		});
	},

	rebuild_instruction_to_bank(frm) {
		if (is_manual_payment(frm)) {
			return;
		}
		const invoices = (frm.doc.invoices || [])
			.map((row) => row.purchase_invoice)
			.filter(Boolean);
		const documents = (frm.doc.invoices || [])
			.map((row) => (row.reference_description || "").trim())
			.filter(Boolean);
		if (!invoices.length && !documents.length) {
			frm.set_value("instruction_to_bank", "");
			return;
		}
		frappe.call({
			method: "erpnext_moldova_banking.utils.bank_payment_instruction.preview_instruction_to_bank",
			args: { purchase_invoices: invoices, documents: documents },
			callback(r) {
				if (r.message) {
					frm.set_value("instruction_to_bank", r.message);
				}
			},
		});
	},

	invoices_remove(frm) {
		sum_allocated_amount(frm);
		frm.trigger("rebuild_instruction_to_bank");
	},

	match_create_payment_entry(frm) {
		frappe.confirm(__("Find matching Bank Transaction and create Payment Entry?"), () => {
			frappe.call({
				method: "erpnext_moldova_banking.utils.maib_payment_match.match_instruction_to_bank_transaction",
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
							message: __("No matching submitted Bank Transaction was found."),
							indicator: "orange",
						});
					}
				},
			});
		});
	},
});

frappe.ui.form.on("Bank Payment Instruction Invoice", {
	purchase_invoice(frm, cdt, cdn) {
		const row = locals[cdt][cdn];
		if (!row.purchase_invoice) {
			sum_allocated_amount(frm);
			return;
		}
		frappe.call({
			method: "erpnext_moldova_banking.utils.bank_payment_instruction.get_purchase_invoice_row",
			args: { purchase_invoice: row.purchase_invoice },
			callback(r) {
				const values = r.message || {};
				frappe.model.set_value(cdt, cdn, "reference_description", values.reference_description || "");
				frappe.model.set_value(cdt, cdn, "outstanding_amount", values.outstanding_amount || 0);
				frappe.model.set_value(cdt, cdn, "allocated_amount", values.outstanding_amount || 0);
				if (values.supplier && frm.doc.party_type === "Supplier" && !frm.doc.party) {
					frm.set_value("party", values.supplier);
				}
				if (values.currency && !frm.doc.currency) {
					frm.set_value("currency", values.currency);
				}
				sum_allocated_amount(frm);
				frm.trigger("rebuild_instruction_to_bank");
			},
		});
	},

	allocated_amount(frm) {
		sum_allocated_amount(frm);
	},

	reference_description(frm, cdt, cdn) {
		const row = locals[cdt][cdn];
		const cleaned = cleanInstructionToBank(row.reference_description || "");
		if (cleaned !== (row.reference_description || "")) {
			frappe.model.set_value(cdt, cdn, "reference_description", cleaned);
			return;
		}
		frm.trigger("rebuild_instruction_to_bank");
	},

	invoices_remove(frm) {
		sum_allocated_amount(frm);
		frm.trigger("rebuild_instruction_to_bank");
	},
});

function cleanInstructionToBank(text) {
	const diacritics = {
		ă: "a",
		â: "a",
		î: "i",
		ș: "s",
		ş: "s",
		ț: "t",
		ţ: "t",
		Ă: "A",
		Â: "A",
		Î: "I",
		Ș: "S",
		Ş: "S",
		Ț: "T",
		Ţ: "T",
	};
	let plain = String(text || "").replace(/[ăâîșşțţĂÂÎȘŞȚŢ]/g, (ch) => diacritics[ch] || ch);
	plain = plain.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
	plain = plain.replace(/[–—−]/g, "-");
	plain = plain.replace(/[^A-Za-z0-9,.\-/ ]+/g, "");
	return plain.replace(/ +/g, " ").trim().slice(0, 210);
}

function invoice_filters(frm) {
	const filters = {
		docstatus: 1,
		outstanding_amount: [">", 0],
	};
	if (frm.doc.company) {
		filters.company = frm.doc.company;
	}
	if (frm.doc.party_type === "Supplier" && frm.doc.party) {
		filters.supplier = frm.doc.party;
	}
	if (frm.doc.currency) {
		filters.currency = frm.doc.currency;
	}
	return { filters };
}

function sum_allocated_amount(frm) {
	let total = 0;
	(frm.doc.invoices || []).forEach((row) => {
		total += flt(row.allocated_amount);
	});
	if ((frm.doc.invoices || []).some((row) => row.purchase_invoice)) {
		frm.set_value("amount", total);
	}
}

function party_bank_account_filters(frm) {
	if (!frm.doc.party_type || !frm.doc.party) {
		return { filters: { name: ["in", [""]] } };
	}
	if (frm.doc.party_type === "Company") {
		return {
			filters: {
				is_company_account: 1,
				company: frm.doc.party,
			},
		};
	}
	return {
		filters: {
			is_company_account: 0,
			party_type: frm.doc.party_type,
			party: frm.doc.party,
		},
	};
}

function keep_beneficiary_fields_visible(frm) {
	["beneficiary_name", "beneficiary_fiscal_code", "destination_iban", "destination_bic"].forEach(
		(fieldname) => {
			const field = frm.get_field(fieldname);
			if (!field || !field.df) {
				return;
			}
			field.df.get_status = () => (cint(field.df.hidden) ? "None" : "Read");
			field.refresh();
		}
	);
}

const MANUAL_PAYMENT_PARTY_TYPES = ["Company", "Shareholder", "Employee"];

function is_manual_payment(frm) {
	return MANUAL_PAYMENT_PARTY_TYPES.includes(frm.doc.party_type);
}

function toggle_manual_payment(frm, clear_invoices) {
	const manual = is_manual_payment(frm);
	frm.toggle_display("section_invoices", !manual);
	frm.toggle_display("invoices", !manual);
	frm.set_df_property("amount", "read_only", manual && frm.doc.docstatus === 0 ? 0 : 1);
	if (manual && clear_invoices && (frm.doc.invoices || []).length) {
		frm.clear_table("invoices");
		frm.refresh_field("invoices");
	}
}

function set_company_bank_account(frm) {
	if (!frm.doc.company || frm.doc.docstatus !== 0) {
		return;
	}
	frappe.call({
		method: "erpnext_moldova_banking.utils.bank_payment_instruction.get_default_company_bank_account",
		args: {
			company: frm.doc.company,
			party_type: frm.doc.party_type,
			party: frm.doc.party,
		},
		callback(r) {
			if (r.message) {
				frm.set_value("company_bank_account", r.message);
			}
		},
	});
}

function clear_beneficiary(frm) {
	frm.set_value("beneficiary_name", "");
	frm.set_value("beneficiary_fiscal_code", "");
	frm.set_value("destination_iban", "");
	frm.set_value("destination_bic", "");
	frm.set_value("residency_status", "N");
}
