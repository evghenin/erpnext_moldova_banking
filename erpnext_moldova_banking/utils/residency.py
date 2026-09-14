# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Ensure Moldova Residency Status exists on Company, Customer, and Supplier.

Another Moldova app (for example erpnext_moldova_efactura) may already own this
field. Never create a duplicate Custom Field when one already exists.
"""

from __future__ import annotations

import frappe
from frappe.custom.doctype.custom_field.custom_field import create_custom_fields

FIELDNAME = "moldova_residency_status"
PARTY_DOCTYPES = ("Company", "Customer", "Supplier")

_FIELD = {
	"fieldname": FIELDNAME,
	"label": "Moldova Residency Status",
	"fieldtype": "Select",
	"options": "Resident\nNon-Resident",
	"default": "Resident",
	"insert_after": "tax_id",
}


def residency_field_exists(doctype: str) -> bool:
	if frappe.db.exists("Custom Field", {"dt": doctype, "fieldname": FIELDNAME}):
		return True
	if frappe.db.exists("DocField", {"parent": doctype, "fieldname": FIELDNAME}):
		return True
	try:
		return bool(frappe.get_meta(doctype).has_field(FIELDNAME))
	except Exception:
		return False


def ensure_moldova_residency_status_fields() -> dict[str, str]:
	"""Create the field only on doctypes that do not already have it.

	Returns a map of doctype -> 'exists' | 'created'.
	"""
	to_create: dict[str, list] = {}
	result: dict[str, str] = {}
	for doctype in PARTY_DOCTYPES:
		if residency_field_exists(doctype):
			result[doctype] = "exists"
			continue
		to_create[doctype] = [dict(_FIELD)]
		result[doctype] = "created"

	if to_create:
		create_custom_fields(to_create, ignore_validate=True, update=False)

	return result
