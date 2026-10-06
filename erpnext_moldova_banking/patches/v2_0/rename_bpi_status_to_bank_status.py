# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Copy Bank Payment Instruction.status into bank_status after schema sync.

Frappe list view drops a field named status on submittable DocTypes, so the
bank state is stored as bank_status. Migrate adds the new column first; this
patch copies existing values and retargets property setters.
"""

import json

import frappe


DOCTYPE = "Bank Payment Instruction"


def execute():
	if not frappe.db.table_exists(DOCTYPE):
		return
	if not frappe.db.has_column(DOCTYPE, "status") or not frappe.db.has_column(DOCTYPE, "bank_status"):
		return

	frappe.db.sql(
		f"""
		UPDATE `tab{DOCTYPE}`
		SET `bank_status` = `status`
		WHERE IFNULL(`bank_status`, '') = ''
			AND IFNULL(`status`, '') != ''
		"""
	)
	frappe.db.sql(
		f"""
		UPDATE `tab{DOCTYPE}`
		SET `bank_status` = 'Waiting For Authorization'
		WHERE `bank_status` = 'Waiting For Authorisation'
		"""
	)

	if frappe.db.table_exists("Property Setter"):
		frappe.db.sql(
			"""
			UPDATE `tabProperty Setter`
			SET field_name = 'bank_status'
			WHERE doc_type = %s AND field_name = 'status'
			""",
			DOCTYPE,
		)

	_rename_list_view_column()


def _rename_list_view_column():
	if not frappe.db.exists("List View Settings", DOCTYPE):
		return
	raw = frappe.db.get_value("List View Settings", DOCTYPE, "fields")
	if not raw:
		return
	try:
		fields = json.loads(raw)
	except ValueError:
		return
	changed = False
	for field in fields:
		if field.get("fieldname") == "status":
			field["fieldname"] = "bank_status"
			changed = True
	if changed:
		frappe.db.set_value("List View Settings", DOCTYPE, "fields", json.dumps(fields), update_modified=False)
