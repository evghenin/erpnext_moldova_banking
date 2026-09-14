# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""One-shot v1 → v2 cleanup.

v1 shipped POS Clearing Rule under the orphan module "ERPNext Moldova Banking".
v2 drops both. Schema adds (MAIB settings, BPI, residency custom fields) are
handled by DocType migrate and after_migrate — no intermediate field-rename
or Payment Order custom-field patches are required.
"""

import frappe


POS_CLEARING_RULE = "Moldova Banking POS Clearing Rule"
ORPHAN_MODULE = "ERPNext Moldova Banking"


def execute():
	if frappe.db.exists("DocType", POS_CLEARING_RULE):
		frappe.delete_doc("DocType", POS_CLEARING_RULE, force=True, ignore_permissions=True)

	if frappe.db.table_exists(POS_CLEARING_RULE):
		frappe.db.sql_ddl(f"DROP TABLE IF EXISTS `tab{POS_CLEARING_RULE}`")

	if not frappe.db.exists("Module Def", ORPHAN_MODULE):
		return

	for dt in ("DocType", "Custom Field", "Property Setter", "Report", "Page", "Workspace"):
		if frappe.db.table_exists(dt) and frappe.db.has_column(dt, "module"):
			frappe.db.sql(
				f"UPDATE `tab{dt}` SET module=%s WHERE module=%s",
				("Moldova Banking", ORPHAN_MODULE),
			)
	frappe.delete_doc("Module Def", ORPHAN_MODULE, force=True, ignore_permissions=True)
