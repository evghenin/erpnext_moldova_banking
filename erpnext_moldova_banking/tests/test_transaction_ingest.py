# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase
from frappe.utils import today

from erpnext_moldova_banking.tests.utils import (
	create_maib_company_bank_account,
	create_submitted_bank_transaction,
	ensure_supplier_tax_id,
)
from erpnext_moldova_banking.utils.transaction_ingest import ingest_transactions

test_dependencies = ["Company", "Cost Center"]


class TestTransactionIngest(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		for doctype in test_dependencies:
			make_test_records(doctype, commit=True)

	def setUp(self):
		self.bank_account, _ = create_maib_company_bank_account()

	def tearDown(self):
		frappe.db.rollback()

	def test_ingest_creates_and_skips_duplicates(self):
		row = {
			"date": today(),
			"deposit": 0,
			"withdrawal": 100.0,
			"description": "Test line",
			"reference_number": f"INGEST-{frappe.generate_hash(length=8)}",
			"currency": "INR",
			"cp_idno": ensure_supplier_tax_id(),
		}
		stats1 = ingest_transactions(self.bank_account, [row], submit=False)
		self.assertEqual(stats1["created"], 1)
		self.assertEqual(stats1["skipped"], 0)
		self.assertEqual(stats1["errors"], 0)

		stats2 = ingest_transactions(self.bank_account, [row], submit=False)
		self.assertEqual(stats2["created"], 0)
		self.assertEqual(stats2["skipped"], 1)
		self.assertEqual(stats2["errors"], 0)

	def test_create_bank_transaction_helper(self):
		bt = create_submitted_bank_transaction(
			bank_account=self.bank_account,
			withdrawal=15.5,
			description="helper",
		)
		self.assertEqual(bt.docstatus, 1)
		self.assertEqual(bt.withdrawal, 15.5)
