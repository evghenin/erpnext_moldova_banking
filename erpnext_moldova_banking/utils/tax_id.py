# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Ensure Tax ID exists on Shareholder and Employee and is the default IDNO field."""

from __future__ import annotations

import frappe
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

FIELDNAME = "tax_id"
PARTY_FIELDS = {
	"Shareholder": "title",
	"Employee": "bank_name",
}


def tax_id_field_exists(doctype: str) -> bool:
	if frappe.db.exists("Custom Field", {"dt": doctype, "fieldname": FIELDNAME}):
		return True
	if frappe.db.exists("DocField", {"parent": doctype, "fieldname": FIELDNAME}):
		return True
	try:
		return bool(frappe.get_meta(doctype).has_field(FIELDNAME))
	except Exception:
		return False


def ensure_party_tax_id_fields() -> dict[str, str]:
	to_create: dict[str, list] = {}
	result: dict[str, str] = {}
	for doctype, insert_after in PARTY_FIELDS.items():
		if tax_id_field_exists(doctype):
			result[doctype] = "exists"
			continue
		to_create[doctype] = [
			{
				"fieldname": FIELDNAME,
				"label": "Tax ID",
				"fieldtype": "Data",
				"unique": 1,
				"insert_after": insert_after,
			}
		]
		result[doctype] = "created"

	if to_create:
		create_custom_fields(to_create, ignore_validate=True, update=False)

	_set_default_idno_settings()
	return result


def _set_default_idno_settings():
	if not frappe.db.exists("DocType", "Moldova Banking Settings"):
		return
	for fieldname in ("shareholder_idno_field", "employee_idno_field"):
		if not frappe.db.get_single_value("Moldova Banking Settings", fieldname):
			frappe.db.set_single_value("Moldova Banking Settings", fieldname, FIELDNAME)
