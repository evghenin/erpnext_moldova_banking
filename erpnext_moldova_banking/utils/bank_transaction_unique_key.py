import hashlib

import frappe
from frappe import _
from frappe.utils import add_days, flt, getdate


def _norm_text(value) -> str:
	return (value or "").strip()


def _signed_amount(deposit, withdrawal) -> float:
	return flt(deposit) - flt(withdrawal)


def make_transaction_unique_key(
	company,
	bank_account,
	posting_date,
	deposit,
	withdrawal,
	reference_number,
	party_name=None,
	currency=None,
):
	"""Build a deterministic unique key for a bank transaction.

	Identity: bank account + document number + date + payer + amount + currency.
	Company is included so the same account name in another company cannot collide.
	"""
	amount = _signed_amount(deposit, withdrawal)
	posting_date_str = ""
	if posting_date:
		posting_date_str = getdate(posting_date).isoformat()
	ref = _norm_text(reference_number)
	party = _norm_text(party_name)
	ccy = _norm_text(currency).upper()

	key = (
		f"{company}::{bank_account}::{posting_date_str}::{amount:.2f}"
		f"::{ref}::{party}::{ccy}"
	)
	# Bank Transaction-unique_key is a Data field (max 140).
	if len(key) > 140:
		return hashlib.sha256(key.encode("utf-8")).hexdigest()
	return key


def find_existing_bank_transaction(
	*,
	bank_account,
	posting_date,
	reference_number,
	party_name,
	deposit,
	withdrawal,
	currency,
	exclude_name=None,
) -> str | None:
	"""Return an existing Bank Transaction matching document, payer, amount, currency.

	Date may differ by one calendar day (API write date vs DBO processed date).
	"""
	if not bank_account or not posting_date:
		return None

	posting_date = getdate(posting_date)
	params = {
		"bank_account": bank_account,
		"date_from": add_days(posting_date, -1),
		"date_to": add_days(posting_date, 1),
		"posting_date": posting_date,
		"reference_number": _norm_text(reference_number),
		"party_name": _norm_text(party_name),
		"deposit": round(flt(deposit), 2),
		"withdrawal": round(flt(withdrawal), 2),
		"currency": _norm_text(currency).upper(),
	}
	exclude_sql = ""
	if exclude_name:
		exclude_sql = " AND name != %(exclude_name)s"
		params["exclude_name"] = exclude_name

	rows = frappe.db.sql(
		f"""
		SELECT name
		FROM `tabBank Transaction`
		WHERE bank_account = %(bank_account)s
			AND `date` BETWEEN %(date_from)s AND %(date_to)s
			AND IFNULL(reference_number, '') = %(reference_number)s
			AND IFNULL(bank_party_name, '') = %(party_name)s
			AND ROUND(IFNULL(deposit, 0), 2) = %(deposit)s
			AND ROUND(IFNULL(withdrawal, 0), 2) = %(withdrawal)s
			AND UPPER(IFNULL(currency, '')) = %(currency)s
			AND docstatus < 2
			{exclude_sql}
		ORDER BY ABS(DATEDIFF(`date`, %(posting_date)s)) ASC, name ASC
		LIMIT 1
		""",
		params,
	)
	return rows[0][0] if rows else None


def ensure_unique_transaction(doc, method=None):
	"""before_insert hook for Bank Transaction.

	Duplicate when document number, payer, amount, and currency already
	exist on the same bank account with date equal or ±1 day.
	"""

	if not getattr(doc, "company", None) and getattr(doc, "bank_account", None):
		doc.company = frappe.db.get_value("Bank Account", doc.bank_account, "company")

	posting_date = getattr(doc, "date", None) or getattr(doc, "posting_date", None)
	reference_number = getattr(doc, "reference_number", None)
	party_name = getattr(doc, "bank_party_name", None)
	currency = getattr(doc, "currency", None)
	deposit = getattr(doc, "deposit", None)
	withdrawal = getattr(doc, "withdrawal", None)

	unique_key = make_transaction_unique_key(
		doc.company,
		doc.bank_account,
		posting_date,
		deposit,
		withdrawal,
		reference_number,
		party_name=party_name,
		currency=currency,
	)
	doc.unique_key = unique_key

	existing = find_existing_bank_transaction(
		bank_account=doc.bank_account,
		posting_date=posting_date,
		reference_number=reference_number,
		party_name=party_name,
		deposit=deposit,
		withdrawal=withdrawal,
		currency=currency,
		exclude_name=getattr(doc, "name", None),
	)
	if not existing and frappe.db.exists("Bank Transaction", {"unique_key": unique_key}):
		existing = frappe.db.get_value("Bank Transaction", {"unique_key": unique_key}, "name")

	if existing:
		amount = _signed_amount(deposit, withdrawal)
		msg = _(
			"Duplicate bank statement line skipped: "
			"Company {0}, Bank Account {1}, Date {2}, Amount {3}, "
			"Reference {4}, Payer {5}, Currency {6}."
		).format(
			doc.company,
			doc.bank_account,
			posting_date,
			f"{amount:.2f}",
			_norm_text(reference_number) or "-",
			_norm_text(party_name) or "-",
			_norm_text(currency).upper() or "-",
		)
		frappe.throw(msg)
