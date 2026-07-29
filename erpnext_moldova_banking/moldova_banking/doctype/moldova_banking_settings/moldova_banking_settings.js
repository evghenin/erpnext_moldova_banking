// ERPNext v15 – Moldova Banking Settings

frappe.ui.form.on("Moldova Banking Settings", {
	onload(frm) {
		set_automation_mop_filter(frm);
		set_maib_sync_account_query(frm);
	},
	refresh(frm) {
		set_automation_mop_filter(frm);
		apply_automation_rules_logic(frm);
		set_options_for_idno_selects(frm);
		set_maib_sync_account_query(frm);
	},
	maib_environment(frm) {
		autofill_maib_endpoint_defaults(frm, true);
	},
	regenerate_bnm_rates_key(frm) {
		const btn = frm.get_field("regenerate_bnm_rates_key")?.$input;
		if (btn) btn.prop("disabled", true);

		frappe
			.call({
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

		frappe
			.call({
				method: "erpnext_moldova_banking.utils.bnm_key.configure_currency_exchange_bnm",
				freeze: true,
				freeze_message: __("Configuring..."),
			})
			.then(() => frappe.show_alert({ message: __("Configured."), indicator: "green" }))
			.finally(() => {
				if (btn) btn.prop("disabled", false);
			});
	},
	test_maib_connection(frm) {
		if (frm.is_dirty()) {
			frappe.msgprint(__("Please save Moldova Banking Settings before testing the connection."));
			return;
		}
		const btn = frm.get_field("test_maib_connection")?.$input;
		if (btn) btn.prop("disabled", true);

		frappe
			.call({
				method: "erpnext_moldova_banking.utils.maib_sync.test_maib_connection",
				freeze: true,
				freeze_message: __("Testing MAIB connection..."),
			})
			.then((r) => {
				const msg = r.message || {};
				frappe.msgprint({
					title: __("MAIB Connection OK"),
					indicator: "green",
					message: __(
						"Authenticated successfully ({0}). Token expires in {1}s.<br>API Base URL: {2}",
						[msg.environment || "", msg.expires_in || "", msg.api_base_url || ""]
					),
				});
			})
			.finally(() => {
				if (btn) btn.prop("disabled", false);
			});
	},
	fetch_maib_statement(frm) {
		if (frm.is_dirty()) {
			frappe.msgprint(__("Please save Moldova Banking Settings before fetching statements."));
			return;
		}
		open_maib_fetch_dialog(frm);
	},
});

function open_maib_fetch_dialog(frm) {
	const dialog = new frappe.ui.Dialog({
		title: __("Fetch MAIB Statement"),
		fields: [
			{
				fieldname: "from_date",
				label: __("From Date"),
				fieldtype: "Date",
				reqd: 1,
				default: frappe.datetime.month_start(),
			},
			{
				fieldname: "to_date",
				label: __("To Date"),
				fieldtype: "Date",
				reqd: 1,
				default: frappe.datetime.get_today(),
			},
			{
				fieldname: "bank_account",
				label: __("Bank Account"),
				fieldtype: "Link",
				options: "Bank Account",
				reqd: 1,
				get_query: () => ({
					filters: {
						is_company_account: 1,
					},
				}),
				onchange: function () {
					const bank_account = dialog.get_value("bank_account");
					if (!bank_account) return;
					frappe
						.call({
							method: "erpnext_moldova_banking.utils.maib_sync.get_sync_account_defaults",
							args: { bank_account },
						})
						.then((r) => {
							dialog.set_value("auto_submit", r.message?.auto_submit || 0);
						});
				},
			},
			{
				fieldname: "auto_submit",
				label: __("Auto Submit"),
				fieldtype: "Check",
				default: 0,
			},
		],
		primary_action_label: __("Run"),
		primary_action(values) {
			if (!values.from_date || !values.to_date || !values.bank_account) {
				frappe.msgprint(__("Please fill all required fields."));
				return;
			}

			dialog.disable_primary_action();
			dialog.get_primary_btn().prop("disabled", true);

			const $status = $('<p class="text-muted"></p>').text(
				__("Fetching statement from MAIB...")
			);
			const $bar = $(
				'<div class="progress-bar" role="progressbar" style="width: 2%; transition: width 0.25s ease;"></div>'
			);
			dialog.$body.addClass("hide");
			dialog.$message
				.removeClass("hide")
				.empty()
				.append(
					$('<div style="padding: 1rem 0;"></div>')
						.append($status)
						.append($('<div class="progress" style="height: 8px;"></div>').append($bar))
				);

			const set_progress = (percent, message) => {
				const pct = Math.max(2, Math.min(100, percent));
				$bar.css("width", pct + "%");
				if (message) {
					$status.text(message);
				}
			};

			const fail = () => {
				dialog.clear_message();
				dialog.$body.removeClass("hide");
				dialog.enable_primary_action();
				dialog.get_primary_btn().prop("disabled", false);
			};

			set_progress(2, __("Fetching statement from MAIB..."));

			frappe
				.call({
					method: "erpnext_moldova_banking.utils.maib_sync.download_maib_statement",
					args: {
						bank_account: values.bank_account,
						from_date: values.from_date,
						to_date: values.to_date,
					},
				})
				.then(async (r) => {
					try {
						const payload = r.message || {};
						const rows = payload.rows || [];
						const pending = rows.filter((row) => row.is_new);
						const fetched = payload.fetched || rows.length || 0;

						set_progress(
							5,
							__("Loaded {0} transactions from the bank", [fetched])
						);

						let created = 0;
						let skipped = rows.length - pending.length;
						let errors = 0;

						if (pending.length === 0) {
							set_progress(100, __("Done"));
						} else {
							const step = 95 / pending.length;
							for (let i = 0; i < pending.length; i++) {
								set_progress(
									5 + step * i,
									__("Loading transaction details ({0}/{1})...", [
										i + 1,
										pending.length,
									])
								);
								try {
									const pr = await frappe.call({
										method:
											"erpnext_moldova_banking.utils.maib_sync.process_maib_statement_row",
										args: {
											bank_account: values.bank_account,
											row: pending[i],
											auto_submit: values.auto_submit ? 1 : 0,
										},
									});
									const stats = pr.message || {};
									created += stats.created || 0;
									skipped += stats.skipped || 0;
									errors += stats.errors || 0;
								} catch (e) {
									errors += 1;
								}
								set_progress(
									5 + step * (i + 1),
									__("Loaded transaction details ({0}/{1})", [
										i + 1,
										pending.length,
									])
								);
							}
						}

						await frappe.call({
							method:
								"erpnext_moldova_banking.utils.maib_sync.finalize_maib_statement_fetch",
							args: {
								bank_account: values.bank_account,
								fetched,
								created,
								skipped,
								errors,
							},
						});

						dialog.hide();
						frappe.msgprint({
							title: __("MAIB Statement Fetch Complete"),
							indicator: errors ? "orange" : "green",
							message: __(
								"Fetched {0} rows from the bank.<br>Created: {1}<br>Skipped duplicates: {2}<br>Errors: {3}",
								[fetched, created, skipped, errors]
							),
						});
						frm.reload_doc();
					} catch (e) {
						fail();
					}
				})
				.catch(() => fail());
		},
	});

	dialog.show();
}

function autofill_maib_endpoint_defaults(frm, force = false) {
	frappe
		.call({
			method: "erpnext_moldova_banking.utils.maib_sync.get_maib_provider_defaults",
			args: { environment: frm.doc.maib_environment || "Test" },
		})
		.then((r) => {
			const defaults = r.message || {};
			const set_if = (fieldname, value) => {
				if (value === undefined || value === null) return;
				if (force || !frm.doc[fieldname]) {
					frm.set_value(fieldname, value);
				}
			};
			set_if("maib_scope", defaults.scope || "payments_gateway");
			set_if("maib_token_url", defaults.token_url || "");
			set_if("maib_api_base_url", defaults.api_base_url || "");
		});
}

function set_maib_sync_account_query(frm) {
	frm.set_query("bank_account", "maib_sync_accounts", () => ({
		filters: {
			is_company_account: 1,
		},
	}));
}

function set_automation_mop_filter(frm) {
	frm.set_query("automation_mode_of_payment", () => ({
		filters: { type: "Bank" },
	}));
}

function apply_automation_rules_logic(frm) {
	const table = frm.fields_dict.automation_rules;
	if (!table || !table.grid) return;

	const grid = table.grid;

	grid.get_field("second_account").get_query = function (doc, cdt, cdn) {
		const row = locals[cdt][cdn];
		if (!row || !row.company) {
			return { filters: { name: "__invalid__" } };
		}
		return {
			filters: {
				company: row.company,
				is_group: 0,
			},
		};
	};

	grid.get_field("cost_center").get_query = function (doc, cdt, cdn) {
		const row = locals[cdt][cdn];
		if (!row || !row.company) {
			return { filters: { name: "__invalid__" } };
		}
		return {
			filters: {
				company: row.company,
			},
		};
	};
}

function on_rule_form_render(frm, cdt, cdn) {
	toggle_second_account_field(frm, cdt, cdn);
	toggle_cost_center_field(frm, cdt, cdn);
}

function on_rule_company_change(frm, cdt, cdn) {
	const row = locals[cdt][cdn];
	if (!row) return;

	if (row.second_account) {
		frappe.model.set_value(cdt, cdn, "second_account", null);
	}
	if (row.cost_center) {
		frappe.model.set_value(cdt, cdn, "cost_center", null);
	}

	toggle_second_account_field(frm, cdt, cdn);
	toggle_cost_center_field(frm, cdt, cdn);
}

function toggle_second_account_field(frm, cdt, cdn) {
	const table = frm.fields_dict.automation_rules;
	if (!table || !table.grid) return;

	const grid_row = table.grid.grid_rows_by_docname[cdn];
	if (!grid_row) return;

	const row = locals[cdt][cdn];
	grid_row.toggle_editable("second_account", !!(row && row.company));
}

function toggle_cost_center_field(frm, cdt, cdn) {
	const table = frm.fields_dict.automation_rules;
	if (!table || !table.grid) return;

	const grid_row = table.grid.grid_rows_by_docname[cdn];
	if (!grid_row) return;

	const row = locals[cdt][cdn];
	grid_row.toggle_editable("cost_center", !!(row && row.company));
}

["Moldova Banking Automation Rule", "Bank Transaction Automation Rule"].forEach((child_doctype) => {
	frappe.ui.form.on(child_doctype, {
		form_render(frm, cdt, cdn) {
			on_rule_form_render(frm, cdt, cdn);
		},
		company(frm, cdt, cdn) {
			on_rule_company_change(frm, cdt, cdn);
		},
	});
});

function set_options_for_idno_selects(frm) {
	frappe.model.with_doctype("Company", () => {
		const fields = frappe.meta.get_docfields("Company");
		const data_fields = fields.filter((df) => df.fieldtype === "Data").map((df) => df.fieldname);
		frm.set_df_property("company_idno_field", "options", [""].concat(data_fields));
	});

	frappe.model.with_doctype("Customer", () => {
		const fields = frappe.meta.get_docfields("Customer");
		const data_fields = fields.filter((df) => df.fieldtype === "Data").map((df) => df.fieldname);
		frm.set_df_property("customer_idno_field", "options", [""].concat(data_fields));
	});

	frappe.model.with_doctype("Supplier", () => {
		const fields = frappe.meta.get_docfields("Supplier");
		const data_fields = fields.filter((df) => df.fieldtype === "Data").map((df) => df.fieldname);
		frm.set_df_property("supplier_idno_field", "options", [""].concat(data_fields));
	});
}
