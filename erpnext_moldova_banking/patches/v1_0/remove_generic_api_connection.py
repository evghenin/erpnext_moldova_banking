import frappe


DOCTYPE = "Moldova Banking API Connection"


def execute():
	"""Remove replaced generic API Connection child DocType."""
	if frappe.db.exists("DocType", DOCTYPE):
		frappe.delete_doc("DocType", DOCTYPE, force=True, ignore_permissions=True)

	if frappe.db.table_exists(DOCTYPE):
		frappe.db.sql_ddl(f"DROP TABLE IF EXISTS `tab{DOCTYPE}`")

	# Drop orphan child rows table link from settings if present as custom column leftovers — N/A for Single child
