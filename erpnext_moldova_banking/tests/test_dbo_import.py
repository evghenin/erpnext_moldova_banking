# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Unit tests for Moldova DBO bank statement import (format from MAIB .txt exports)."""

from __future__ import annotations

import frappe
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.moldova_banking.doctype.moldova_bank_statement_import.moldova_bank_statement_import import (
	has_account_info,
	is_dbo_format,
	parse_date,
	parse_dbo,
)
from erpnext_moldova_banking.tests.utils import create_maib_company_bank_account, load_fixture

test_dependencies = ["Company", "Cost Center"]

IBAN = "MD46AG000000022516020091"


class TestDboImport(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		for doctype in test_dependencies:
			make_test_records(doctype, commit=True)

	def setUp(self):
		self.bank_account, self.gl_account = create_maib_company_bank_account(
			iban_account_id="22516020091"
		)
		# Force exact IBAN from the sample DBO (create helper builds MD24AG… by default)
		frappe.db.set_value(
			"Bank Account",
			self.bank_account,
			{
				"iban": IBAN,
				"is_company_account": 1,
				"company": "_Test Company",
			},
		)
		self.import_doc = frappe._dict(bank_account=self.bank_account)

	def tearDown(self):
		frappe.db.rollback()

	def test_is_dbo_format_current_sample(self):
		content = load_fixture("dbo_current_account_sample.txt")
		self.assertTrue(is_dbo_format(content))
		# New MAIB exports used for tests have no SECTIONACCOUNT block
		self.assertFalse(has_account_info(content))

	def test_is_dbo_format_empty_card_sample(self):
		"""Card batch file with only date range is not a valid DBO statement body."""
		content = load_fixture("dbo_card_empty_sample.txt")
		self.assertFalse(is_dbo_format(content))
		self.assertFalse(has_account_info(content))

	def test_parse_date(self):
		self.assertEqual(str(parse_date("24.07.2026")), "2026-07-24")
		self.assertEqual(str(parse_date("2026-07-24")), "2026-07-24")
		self.assertIsNone(parse_date(""))
		self.assertIsNone(parse_date("not-a-date"))

	def test_parse_dbo_withdrawal_and_deposit(self):
		content = load_fixture("dbo_current_account_sample.txt")
		rows = parse_dbo(content, self.import_doc)
		self.assertEqual(len(rows), 2)

		withdrawal = rows[0]
		self.assertEqual(withdrawal["withdrawal"], 3.8)
		self.assertEqual(withdrawal["deposit"], 0)
		self.assertEqual(withdrawal["reference_number"], "5341")
		self.assertEqual(str(withdrawal["date"]), "2026-07-24")
		self.assertEqual(withdrawal["cp_role"], "Receiver")
		self.assertIn("Amount: 3.80", withdrawal["description"])
		self.assertIn("Document Number: 5341", withdrawal["description"])
		self.assertIn("Com. Plati Ord.PJ Internet Banking", withdrawal["description"])

		deposit = rows[1]
		self.assertEqual(deposit["deposit"], 3105.0)
		self.assertEqual(deposit["withdrawal"], 0)
		self.assertEqual(deposit["reference_number"], "1709")
		self.assertEqual(deposit["cp_role"], "Payer")
		self.assertEqual(deposit["cp_idno"], "1000000000003")
		self.assertIn("PAYER COMPANY SRL", deposit["cp_name"])
		# Running balance: 0 opening (no STARTREST) → -3.80 → +3105
		self.assertAlmostEqual(deposit["bank_balance"], 3105.0 - 3.8)

	def test_parse_dbo_bank_iban_must_match_documents(self):
		"""Sample DBO has no ACCOUNT= header; company IBAN is taken from Bank Account."""
		frappe.db.set_value("Bank Account", self.bank_account, "iban", "MD00AG000000099999999999")
		content = load_fixture("dbo_current_account_sample.txt")
		with self.assertRaises(frappe.ValidationError) as ctx:
			parse_dbo(content, self.import_doc)
		self.assertIn("not belongs", str(ctx.exception).lower())

	def test_parse_dbo_rejects_foreign_only_document(self):
		"""Document where neither payer nor receiver is our IBAN must fail."""
		content = """BEGINDATE=24.07.2026
ENDDATE=24.07.2026
DocStart
DOCUMENTNUMBER=1
DOCUMENTDATE=24.07.2026
DATEWRITTEN=24.07.2026
PAYERACCOUNT=MD11AG000000011111111111
PAYER=OTHER
RECEIVERACCOUNT=MD22AG000000022222222222
RECEIVER=OTHER2
AMOUNT=10.00
GROUND=x
DocEnd
"""
		with self.assertRaises(frappe.ValidationError) as ctx:
			parse_dbo(content, self.import_doc)
		self.assertIn("not belongs", str(ctx.exception).lower())
