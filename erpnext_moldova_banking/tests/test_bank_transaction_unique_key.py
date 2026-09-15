# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.utils.bank_transaction_unique_key import make_transaction_unique_key


class TestBankTransactionUniqueKey(FrappeTestCase):
	def test_key_includes_document_date_payer_amount_currency(self):
		key = make_transaction_unique_key(
			"_Test Company",
			"BA-1",
			"2026-09-01",
			4530,
			0,
			"1982",
			party_name="(R) S.R.L. 'NEW AGE'",
			currency="mdl",
		)
		self.assertEqual(
			key,
			"_Test Company::BA-1::2026-09-01::4530.00::1982::(R) S.R.L. 'NEW AGE'::MDL",
		)

	def test_different_payer_or_currency_is_a_different_key(self):
		base = dict(
			company="_Test Company",
			bank_account="BA-1",
			posting_date="2026-09-01",
			deposit=100,
			withdrawal=0,
			reference_number="10",
			party_name="A SRL",
			currency="MDL",
		)
		same = make_transaction_unique_key(**base)
		other_payer = make_transaction_unique_key(**{**base, "party_name": "B SRL"})
		other_ccy = make_transaction_unique_key(**{**base, "currency": "EUR"})
		self.assertNotEqual(same, other_payer)
		self.assertNotEqual(same, other_ccy)
