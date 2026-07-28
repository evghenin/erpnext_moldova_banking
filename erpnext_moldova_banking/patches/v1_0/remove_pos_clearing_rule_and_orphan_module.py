import frappe


DOCTYPE = "Moldova Banking POS Clearing Rule"
ORPHAN_MODULE = "ERPNext Moldova Banking"


def execute():
	"""Remove unused POS Clearing Rule DocType and orphan Module Def."""
	if frappe.db.exists("DocType", DOCTYPE):
		frappe.delete_doc("DocType", DOCTYPE, force=True, ignore_permissions=True)

	# Drop leftover table if DocType delete left it behind
	table = f"tab{DOCTYPE}"
	if frappe.db.table_exists(DOCTYPE):
		frappe.db.sql_ddl(f"DROP TABLE IF EXISTS `{table}`")

	if frappe.db.exists("Module Def", ORPHAN_MODULE):
		# Reassign anything still pointing at the orphan module, then delete it
		for dt in ("DocType", "Custom Field", "Property Setter", "Report", "Page", "Workspace"):
			if frappe.db.table_exists(dt) and frappe.db.has_column(dt, "module"):
				frappe.db.sql(
					f"UPDATE `tab{dt}` SET module=%s WHERE module=%s",
					("Moldova Banking", ORPHAN_MODULE),
				)
		frappe.delete_doc("Module Def", ORPHAN_MODULE, force=True, ignore_permissions=True)
