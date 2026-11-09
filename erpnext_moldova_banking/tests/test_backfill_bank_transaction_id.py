# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

import frappe
from frappe.test_runner import make_test_records
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_days, getdate, today

from erpnext_moldova_banking.patches.v3_0.backfill_bank_transaction_id import (
	apply_statement_transaction_ids,
	iter_statement_chunks,
)
from erpnext_moldova_banking.tests.utils import create_maib_company_bank_account
from erpnext_moldova_banking.utils.maib_sync import _transfer_details_belong_to_row

test_dependencies = ["Company", "Cost Center"]


class TestBackfillBankTransactionId(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		for doctype in test_dependencies:
			make_test_records(doctype, commit=True)

	def setUp(self):
		self.bank_account, _ = create_maib_company_bank_account()

	def tearDown(self):
		frappe.db.rollback()

	def test_month_chunks_cover_the_range(self):
		chunks = list(iter_statement_chunks("2026-01-15", "2026-03-02"))
		self.assertEqual(
			[(str(start), str(end)) for start, end in chunks],
			[
				("2026-01-15", "2026-01-31"),
				("2026-02-01", "2026-02-28"),
				("2026-03-01", "2026-03-02"),
			],
		)

	def test_apply_writes_one_match_and_skips_ambiguous_or_distant(self):
		reference = f"DOC-{frappe.generate_hash(length=6)}"
		kept = _insert_bank_transaction(
			self.bank_account,
			date=today(),
			reference_number=reference,
			party_name="Payer SRL",
			deposit=80,
		)
		monday = _insert_bank_transaction(
			self.bank_account,
			date=add_days(today(), 8),
			reference_number=reference,
			party_name="Payer SRL",
			deposit=80,
		)
		frappe.db.set_value(
			"Bank Transaction",
			monday.name,
			"date",
			getdate(add_days(today(), 2)),
			update_modified=False,
		)

		stats = apply_statement_transaction_ids(
			self.bank_account,
			[
				{
					"date": today(),
					"document_number": reference,
					"transaction_id": "213681697437387.000002",
					"cp_name": "Payer SRL",
					"deposit": 80,
					"withdrawal": 0,
					"currency": "INR",
				}
			],
		)
		self.assertEqual(stats["updated"], 1)
		self.assertEqual(
			frappe.db.get_value("Bank Transaction", kept.name, "transaction_id"),
			"213681697437387.000002",
		)
		self.assertEqual(str(kept.date), str(getdate(today())))
		self.assertFalse(frappe.db.get_value("Bank Transaction", monday.name, "transaction_id"))

		ambiguous_ref = f"DOC-{frappe.generate_hash(length=6)}"
		_insert_bank_transaction(
			self.bank_account,
			date=today(),
			reference_number=ambiguous_ref,
			party_name="Same SRL",
			deposit=10,
		)
		near = _insert_bank_transaction(
			self.bank_account,
			date=add_days(today(), 8),
			reference_number=ambiguous_ref,
			party_name="Same SRL",
			deposit=10,
		)
		frappe.db.set_value(
			"Bank Transaction",
			near.name,
			"date",
			getdate(add_days(today(), 1)),
			update_modified=False,
		)
		ambiguous = apply_statement_transaction_ids(
			self.bank_account,
			[
				{
					"date": today(),
					"document_number": ambiguous_ref,
					"transaction_id": "214498114755446.030002",
					"cp_name": "Same SRL",
					"deposit": 10,
					"withdrawal": 0,
					"currency": "INR",
				}
			],
		)
		self.assertEqual(ambiguous["updated"], 0)
		self.assertEqual(ambiguous["ambiguous"], 1)
		self.assertFalse(
			frappe.db.exists(
				"Bank Transaction",
				{"bank_account": self.bank_account, "transaction_id": "214498114755446.030002"},
			)
		)

	def test_transfer_details_allow_operational_date_shift(self):
		row = {"deposit": 10, "cp_idno": "1000000000005", "date": "2026-11-07", "amount": 10}
		details = {"PayerFiscalCode": "1000000000005", "PayerAmount": "10", "Date": "2026-11-09"}
		self.assertTrue(_transfer_details_belong_to_row(row, details))

		details["Date"] = "2026-10-01"
		self.assertFalse(_transfer_details_belong_to_row(row, details))


def _insert_bank_transaction(bank_account, *, date, reference_number, party_name, deposit):
	doc = frappe.get_doc(
		{
			"doctype": "Bank Transaction",
			"date": getdate(date),
			"bank_account": bank_account,
			"company": "_Test Company",
			"deposit": deposit,
			"withdrawal": 0,
			"reference_number": reference_number,
			"bank_party_name": party_name,
			"currency": "INR",
			"description": reference_number,
		}
	)
	doc.insert(ignore_permissions=True)
	return doc
