# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from datetime import date
from decimal import Decimal
from unittest.mock import patch

import frappe
from frappe.tests.utils import FrappeTestCase

from erpnext_moldova_banking.api.bnm_rates import (
	_calc_rate_via_mdl,
	_fetch_bnm_rates,
	get_bnm_rates_cached,
)

EUR_XML = """<?xml version="1.0" encoding="UTF-8"?>
<ValCurs Date="16.09.2026" name="Official exchange rate">
  <Valute ID="47">
    <NumCode>978</NumCode>
    <CharCode>EUR</CharCode>
    <Nominal>1</Nominal>
    <Name>Euro</Name>
    <Value>20.1111</Value>
  </Valute>
  <Valute ID="44">
    <NumCode>840</NumCode>
    <CharCode>USD</CharCode>
    <Nominal>1</Nominal>
    <Name>US Dollar</Name>
    <Value>17.4000</Value>
  </Valute>
</ValCurs>
"""


class _Resp:
	def __init__(self, text: str, status: int = 200):
		self.text = text
		self.status_code = status

	def raise_for_status(self):
		if self.status_code >= 400:
			raise RuntimeError(f"HTTP {self.status_code}")


class TestBnmRates(FrappeTestCase):
	def tearDown(self):
		frappe.db.rollback()

	def test_calc_eur_to_mdl(self):
		rates = {"EUR": Decimal("20.1111"), "USD": Decimal("17.4")}
		self.assertAlmostEqual(_calc_rate_via_mdl(rates, "EUR", "MDL"), 20.1111)
		self.assertAlmostEqual(_calc_rate_via_mdl(rates, "MDL", "EUR"), 1 / 20.1111)
		self.assertEqual(_calc_rate_via_mdl(rates, "EUR", "EUR"), 1.0)

	def test_empty_xml_returns_no_rates(self):
		with patch("erpnext_moldova_banking.api.bnm_rates.requests.get", return_value=_Resp("")):
			self.assertEqual(_fetch_bnm_rates(date(2026, 9, 17)), {})

	def test_lookback_when_requested_date_unpublished(self):
		def fake_get(url, params=None, timeout=None):
			bnm_date = (params or {}).get("date")
			if bnm_date == "17.09.2026":
				return _Resp("")
			if bnm_date == "16.09.2026":
				return _Resp(EUR_XML)
			return _Resp("")

		with patch("erpnext_moldova_banking.api.bnm_rates.requests.get", side_effect=fake_get):
			rates = get_bnm_rates_cached(date(2026, 9, 17), lookback_days=3)
		self.assertEqual(rates["EUR"], Decimal("20.1111"))

	def test_should_use_bnm_for_mdl_pairs(self):
		from erpnext_moldova_banking.overrides.exchange_rate import _should_use_bnm

		self.assertTrue(_should_use_bnm("EUR", "MDL"))
		self.assertTrue(_should_use_bnm("MDL", "EUR"))
