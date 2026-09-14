# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.utils.bank_payment_instruction import map_residency_status
from erpnext_moldova_banking.utils.residency import (
	FIELDNAME,
	PARTY_DOCTYPES,
	ensure_moldova_residency_status_fields,
	residency_field_exists,
)


class TestMoldovaResidencyStatus(FrappeTestCase):
	def tearDown(self):
		frappe.db.rollback()

	def test_map_residency_status(self):
		self.assertEqual(map_residency_status("Resident"), "R")
		self.assertEqual(map_residency_status("Non-Resident"), "N")
		self.assertEqual(map_residency_status(None), "N")
		self.assertEqual(map_residency_status(""), "N")

	def test_ensure_skips_existing_custom_fields(self):
		first = ensure_moldova_residency_status_fields()
		for doctype in PARTY_DOCTYPES:
			self.assertIn(first[doctype], ("exists", "created"))
			self.assertTrue(residency_field_exists(doctype))

		names_before = {
			doctype: frappe.db.get_value("Custom Field", {"dt": doctype, "fieldname": FIELDNAME}, "name")
			for doctype in PARTY_DOCTYPES
		}
		second = ensure_moldova_residency_status_fields()
		self.assertEqual(second, {doctype: "exists" for doctype in PARTY_DOCTYPES})
		for doctype in PARTY_DOCTYPES:
			self.assertEqual(
				frappe.db.get_value("Custom Field", {"dt": doctype, "fieldname": FIELDNAME}, "name"),
				names_before[doctype],
			)
			self.assertEqual(
				frappe.db.count("Custom Field", {"dt": doctype, "fieldname": FIELDNAME}),
				1,
			)
