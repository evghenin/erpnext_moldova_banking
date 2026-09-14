# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.utils.payment_details import (
	build_instruction_to_bank,
	build_instruction_to_bank_for_invoices,
	clean_instruction_to_bank,
	strip_diacritics,
)


class TestPaymentDetails(FrappeTestCase):
	def test_strip_diacritics(self):
		self.assertEqual(strip_diacritics("Servicii de arenda"), "Servicii de arenda")
		self.assertEqual(strip_diacritics("Știință"), "Stiinta")
		self.assertEqual(strip_diacritics("Țară"), "Tara")

	def test_clean_instruction_to_bank_keeps_letters_digits_comma_dot_dash(self):
		self.assertEqual(
			clean_instruction_to_bank("Plată pentru mărfuri, e-Factura AA-123 din 25.08.2026 / f/n"),
			"Plata pentru marfuri, e-Factura AA-123 din 25.08.2026 / f/n",
		)

	@patch("erpnext_moldova_banking.utils.payment_details.get_pi_item_group_labels", return_value="Servicii")
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_factura", return_value=None)
	@patch(
		"erpnext_moldova_banking.utils.payment_details._linked_purchase_efactura",
		return_value={
			"ef_series": "AA",
			"ef_number": "123",
			"issue_date": "2026-09-02",
			"docstatus": 1,
		},
	)
	@patch(
		"erpnext_moldova_banking.utils.payment_details.frappe.db.get_value",
		return_value={"name": "PINV-1", "posting_date": "2026-09-01"},
	)
	def test_pef_template(self, *_mocks):
		text = build_instruction_to_bank("PINV-1")
		self.assertEqual(text, "Plata pentru Servicii conf. e-Factura nr. AA123 din 02.09.2026")

	@patch("erpnext_moldova_banking.utils.payment_details.get_pi_item_group_labels", return_value="Marfuri")
	@patch(
		"erpnext_moldova_banking.utils.payment_details._linked_purchase_factura",
		return_value={
			"f_series": "B",
			"f_number": "9",
			"issue_date": "2026-08-31",
			"docstatus": 1,
		},
	)
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_efactura", return_value=None)
	@patch(
		"erpnext_moldova_banking.utils.payment_details.frappe.db.get_value",
		return_value={"name": "PINV-1", "posting_date": "2026-09-01"},
	)
	def test_pf_template(self, *_mocks):
		text = build_instruction_to_bank("PINV-1")
		self.assertEqual(text, "Plata pentru Marfuri conf. factura nr. B9 din 31.08.2026")

	@patch("erpnext_moldova_banking.utils.payment_details.get_pi_item_group_labels", return_value="Servicii")
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_factura", return_value=None)
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_efactura", return_value=None)
	@patch(
		"erpnext_moldova_banking.utils.payment_details.frappe.db.get_value",
		return_value={"name": "PINV-1", "posting_date": "2026-09-02"},
	)
	def test_fallback_unknown(self, *_mocks):
		text = build_instruction_to_bank("PINV-1")
		self.assertEqual(text, "Plata pentru Servicii conf. factura nr. f/n din f/d")

	@patch("erpnext_moldova_banking.utils.payment_details.get_pi_item_group_labels", return_value="Servicii")
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_factura", return_value=None)
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_efactura", return_value=None)
	@patch(
		"erpnext_moldova_banking.utils.payment_details.frappe.db.get_value",
		return_value={"name": "PINV-1", "posting_date": "2026-09-02", "bill_no": "445", "bill_date": None},
	)
	def test_fallback_bill_no_only(self, *_mocks):
		text = build_instruction_to_bank("PINV-1")
		self.assertEqual(text, "Plata pentru Servicii conf. factura nr. 445 din f/d")

	@patch("erpnext_moldova_banking.utils.payment_details.get_pi_item_group_labels", return_value="Servicii")
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_factura", return_value=None)
	@patch("erpnext_moldova_banking.utils.payment_details._linked_purchase_efactura", return_value=None)
	@patch(
		"erpnext_moldova_banking.utils.payment_details.frappe.db.get_value",
		return_value={
			"name": "PINV-1",
			"posting_date": "2026-09-02",
			"bill_no": "445",
			"bill_date": "2026-08-20",
		},
	)
	def test_fallback_bill_no_and_date(self, *_mocks):
		text = build_instruction_to_bank("PINV-1")
		self.assertEqual(text, "Plata pentru Servicii conf. factura nr. 445 din 20.08.2026")

	@patch(
		"erpnext_moldova_banking.utils.payment_details.get_invoice_payment_purpose",
		side_effect=[
			{
				"product_types": "Servicii",
				"bill_type": "e-Factura",
				"bill_no": "AA123",
				"bill_date": "02.09.2026",
			},
			{
				"product_types": "Marfuri",
				"bill_type": "factură",
				"bill_no": "B9",
				"bill_date": "31.08.2026",
			},
		],
	)
	def test_multi_invoice_joins_refs(self, *_mocks):
		text = build_instruction_to_bank_for_invoices(["PINV-1", "PINV-2"])
		self.assertEqual(
			text,
			"Plata pentru Servicii, Marfuri conf. e-Factura nr. AA123 din 02.09.2026, factura nr. B9 din 31.08.2026",
		)

	@patch(
		"erpnext_moldova_banking.utils.payment_details.get_invoice_payment_purpose",
		side_effect=[
			{
				"product_types": "Servicii de consultanta si altele",
				"bill_type": "e-Factura",
				"bill_no": "VERYLONGREF" + str(i),
				"bill_date": "01.09.2026",
				"document": f"e-Factura nr. VERYLONGREF{i} din 01.09.2026",
			}
			for i in range(8)
		],
	)
	def test_multi_invoice_compacts_when_too_long(self, *_mocks):
		names = [f"PINV-{i}" for i in range(8)]
		text = build_instruction_to_bank_for_invoices(names)
		self.assertTrue(text.startswith("Plata pentru Servicii de consultanta si altele conf. "))
		self.assertLessEqual(len(text), 210)
		self.assertIn("VERYLONGREF0", text)
