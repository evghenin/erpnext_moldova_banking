# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Use BNM in-process so Purchase Order FX does not HTTP-loopback to this site."""

from __future__ import annotations

import frappe
from frappe import _
from frappe.utils import add_days, flt, get_datetime_str, getdate, nowdate

from erpnext.setup.utils import get_pegged_currencies, get_pegged_rate

from erpnext_moldova_banking.api.bnm_rates import _calc_rate_via_mdl, get_bnm_rates_cached
from erpnext_moldova_banking.utils.bnm_key import BNM_METHOD_PATH, _normalize_path

_PATCHED_ATTR = "_moldova_bnm_exchange_rate"


def _from_currency_exchange_doctype(from_currency, to_currency, transaction_date, args):
	currency_settings = frappe.get_cached_doc("Accounts Settings")
	allow_stale_rates = currency_settings.get("allow_stale")

	filters = [
		["date", "<=", get_datetime_str(transaction_date)],
		["from_currency", "=", from_currency],
		["to_currency", "=", to_currency],
	]

	if args == "for_buying":
		filters.append(["for_buying", "=", "1"])
	elif args == "for_selling":
		filters.append(["for_selling", "=", "1"])

	if not allow_stale_rates:
		stale_days = currency_settings.get("stale_days")
		checkpoint_date = add_days(transaction_date, -stale_days)
		filters.append(["date", ">", get_datetime_str(checkpoint_date)])

	entries = frappe.get_all(
		"Currency Exchange", fields=["exchange_rate"], filters=filters, order_by="date desc", limit=1
	)
	if entries:
		return flt(entries[0].exchange_rate)
	return None


def _ces_points_to_bnm() -> bool:
	try:
		endpoint = frappe.db.get_single_value("Currency Exchange Settings", "api_endpoint") or ""
	except Exception:
		return False
	path = _normalize_path(endpoint)
	return path == BNM_METHOD_PATH or "bnm_rates.get_exchange_rate" in str(endpoint)


def _should_use_bnm(from_currency: str, to_currency: str) -> bool:
	if _ces_points_to_bnm():
		return True
	pair = {(from_currency or "").upper(), (to_currency or "").upper()}
	return "MDL" in pair


def _bnm_rate(from_currency, to_currency, transaction_date) -> float:
	dt = getdate(transaction_date)
	rates = get_bnm_rates_cached(dt)
	return float(_calc_rate_via_mdl(rates, from_currency, to_currency))


@frappe.whitelist()
def get_exchange_rate(from_currency, to_currency, transaction_date=None, args=None):
	if not (from_currency and to_currency):
		return
	if from_currency == to_currency:
		return 1

	if not transaction_date:
		transaction_date = nowdate()

	manual = _from_currency_exchange_doctype(from_currency, to_currency, transaction_date, args)
	if manual is not None:
		return manual

	if frappe.get_single_value("Currency Exchange Settings", "disabled"):
		return 0.00

	currency_settings = frappe.get_cached_doc("Accounts Settings")
	pegged_currencies = {}
	if currency_settings.allow_pegged_currencies_exchange_rates:
		pegged_currencies = get_pegged_currencies()
		if rate := get_pegged_rate(pegged_currencies, from_currency, to_currency, transaction_date):
			return rate

	src = from_currency
	dst = to_currency
	if pegged_currencies:
		if from_currency in pegged_currencies:
			src = pegged_currencies[from_currency]["pegged_against"]
		if to_currency in pegged_currencies:
			dst = pegged_currencies[to_currency]["pegged_against"]

	if _should_use_bnm(src, dst):
		try:
			cache = frappe.cache()
			cache_key = f"bnm_currency_exchange_rate_{transaction_date}:{from_currency}:{to_currency}"
			cached = cache.get(cache_key)
			if cached:
				value = flt(cached)
			else:
				value = flt(_bnm_rate(src, dst, transaction_date))
				cache.setex(name=cache_key, time=21600, value=value)

			if currency_settings.allow_pegged_currencies_exchange_rates and to_currency in pegged_currencies:
				value *= flt(pegged_currencies[to_currency]["ratio"])
			if currency_settings.allow_pegged_currencies_exchange_rates and from_currency in pegged_currencies:
				value /= flt(pegged_currencies[from_currency]["ratio"])
			return flt(value)
		except Exception:
			frappe.log_error(_("Unable to fetch BNM exchange rate"))
			frappe.msgprint(
				_(
					"Unable to find exchange rate for {0} to {1} for key date {2}. Please create a Currency Exchange record manually"
				).format(from_currency, to_currency, transaction_date)
			)
			return 0.0

	from erpnext.setup import utils as erpnext_utils

	original = getattr(erpnext_utils, "_unpatched_get_exchange_rate", None)
	if original and original is not get_exchange_rate:
		return original(from_currency, to_currency, transaction_date=transaction_date, args=args)

	frappe.msgprint(
		_(
			"Unable to find exchange rate for {0} to {1} for key date {2}. Please create a Currency Exchange record manually"
		).format(from_currency, to_currency, transaction_date)
	)
	return 0.0


def apply_patch() -> None:
	import erpnext.controllers.accounts_controller as accounts_controller
	import erpnext.setup.utils as utils
	import erpnext.stock.get_item_details as get_item_details

	if getattr(utils.get_exchange_rate, _PATCHED_ATTR, False):
		return

	if not getattr(utils, "_unpatched_get_exchange_rate", None):
		utils._unpatched_get_exchange_rate = utils.get_exchange_rate

	setattr(get_exchange_rate, _PATCHED_ATTR, True)
	utils.get_exchange_rate = get_exchange_rate
	accounts_controller.get_exchange_rate = get_exchange_rate
	get_item_details.get_exchange_rate = get_exchange_rate
