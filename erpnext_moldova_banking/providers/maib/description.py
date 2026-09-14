# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Build Bank Transaction.description for MAIB statement / transfer details.

Aligned with DBO import format:
  ground
  <blank>
  Amount / Document Number / Date Written
  Payer or Receiver block (name, IDNO, account, bank, BIC)
  OpType / TxnCode
  extra API-only fields (omitted when empty)
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from frappe.utils import flt, getdate


def _detail(details: dict[str, str] | None, *keys: str) -> str:
	if not details:
		return ""
	lower_map = {k.lower(): v for k, v in details.items() if v}
	for key in keys:
		value = lower_map.get(key.lower())
		if value:
			return value.strip()
	return ""


def _mark_used(used: set[str], details: dict[str, str] | None, *keys: str) -> None:
	if not details:
		return
	lower_map = {k.lower(): k for k in details}
	for key in keys:
		original = lower_map.get(key.lower())
		if original:
			used.add(original)


def _append_party(lines: list[str], role: str, name="", idno="", account="", bank="", bic=""):
	if not (name or idno or account or bank or bic):
		return
	if name:
		lines.append(f"{role}: {name}")
	if idno:
		lines.append(f"{role} IDNO: {idno}")
	if account:
		lines.append(f"{role} Account: {account}")
	if bank:
		lines.append(f"{role} Bank: {bank}")
	if bic:
		lines.append(f"{role} Bank BIC: {bic}")


def _format_date_written(value) -> str:
	if value is None:
		return ""
	if hasattr(value, "strftime"):
		try:
			return value.strftime("%d.%m.%Y")
		except Exception:
			return str(value).strip()

	text = str(value).strip()
	if not text:
		return ""
	if re.match(r"^\d{2}\.\d{2}\.\d{4}$", text):
		return text
	if re.match(r"^\d{8}$", text):
		try:
			return datetime.strptime(text, "%Y%m%d").strftime("%d.%m.%Y")
		except ValueError:
			return text
	try:
		return getdate(text).strftime("%d.%m.%Y")
	except Exception:
		return text


def _label_from_tag(tag: str) -> str:
	spaced = re.sub(r"(?<!^)([A-Z])", r" \1", tag or "")
	return spaced.replace("_", " ").strip() or tag


def build_maib_transaction_description(
	row: dict[str, Any] | None = None,
	*,
	transfer_details: dict[str, str] | None = None,
	company_iban: str = "",
	company_name: str = "",
	company_idno: str = "",
) -> str:
	"""Compose DBO-style description from statement row and optional Transfer Details.

	company_* arguments are accepted for call-site compatibility and are not
	written into the description (DBO import only shows the counterparty).
	"""
	row = row or {}
	details = transfer_details or row.get("transfer_details") or {}
	used: set[str] = set()
	_ = (company_iban, company_name, company_idno)

	payment_destination = (
		_detail(details, "PaymentDestination") or (row.get("payment_destination") or "") or ""
	).strip()
	if payment_destination:
		_mark_used(used, details, "PaymentDestination")
	if not payment_destination:
		raw = (row.get("description") or "").strip()
		if raw and "\nAmount:" not in raw and not raw.startswith("Amount:"):
			payment_destination = raw.split("\n\n", 1)[0].strip()

	amount = row.get("amount")
	if amount is None:
		amount = flt(row.get("deposit") or 0) or flt(row.get("withdrawal") or 0)
	if not amount:
		amount = flt(_detail(details, "PayerAmount") or 0)
	if amount:
		_mark_used(used, details, "PayerAmount")

	document_number = (
		_detail(details, "DocumentNumber") or (row.get("document_number") or "")
	).strip()
	if _detail(details, "DocumentNumber"):
		_mark_used(used, details, "DocumentNumber")

	transaction_id = (row.get("transaction_id") or "").strip()
	currency = (_detail(details, "Currency") or (row.get("currency") or "")).strip()
	if _detail(details, "Currency"):
		_mark_used(used, details, "Currency")

	date_written = _format_date_written(_detail(details, "Date") or row.get("date"))
	if _detail(details, "Date"):
		_mark_used(used, details, "Date")

	credit_or_debit = (row.get("credit_or_debit") or "").strip().upper()
	if not credit_or_debit:
		if flt(row.get("deposit") or 0):
			credit_or_debit = "C"
		elif flt(row.get("withdrawal") or 0):
			credit_or_debit = "D"

	lines: list[str] = []
	if payment_destination:
		lines.append(payment_destination)
		lines.append("")

	if amount:
		lines.append(f"Amount: {flt(amount):.2f}")
	if document_number:
		lines.append(f"Document Number: {document_number}")
	if date_written:
		lines.append(f"Date Written: {date_written}")

	payer_fields = {
		"name": _detail(details, "PayerName") or (row.get("cp_name") if credit_or_debit.startswith("C") else ""),
		"idno": _detail(details, "PayerFiscalCode")
		or (row.get("cp_idno") if credit_or_debit.startswith("C") else ""),
		"account": _detail(details, "PayerAccount")
		or (row.get("cp_account") if credit_or_debit.startswith("C") else ""),
		"bank": _detail(details, "PayerBank")
		or (row.get("cp_bank") if credit_or_debit.startswith("C") else ""),
		"bic": _detail(details, "PayerBankCode")
		or (row.get("cp_bank_bic") if credit_or_debit.startswith("C") else ""),
	}
	receiver_fields = {
		"name": _detail(details, "BeneficiaryName")
		or (row.get("cp_name") if credit_or_debit.startswith("D") else ""),
		"idno": _detail(details, "BeneficiaryFiscalCode")
		or (row.get("cp_idno") if credit_or_debit.startswith("D") else ""),
		"account": _detail(details, "BeneficiaryAccount")
		or (row.get("cp_account") if credit_or_debit.startswith("D") else ""),
		"bank": _detail(details, "BeneficiaryBank")
		or (row.get("cp_bank") if credit_or_debit.startswith("D") else ""),
		"bic": _detail(details, "BeneficiaryBankCode")
		or (row.get("cp_bank_bic") if credit_or_debit.startswith("D") else ""),
	}

	# Statement-only counterparty when transfer details have no party names.
	if not _detail(details, "PayerName") and not _detail(details, "BeneficiaryName"):
		cp_name = (row.get("cp_name") or "").strip()
		cp_idno = (row.get("cp_idno") or "").strip()
		cp_account = (row.get("cp_account") or "").strip()
		cp_bank = (row.get("cp_bank") or "").strip()
		cp_bank_bic = (row.get("cp_bank_bic") or "").strip()
		if credit_or_debit.startswith("C"):
			payer_fields = {
				"name": cp_name,
				"idno": cp_idno,
				"account": cp_account,
				"bank": cp_bank,
				"bic": cp_bank_bic,
			}
			receiver_fields = {"name": "", "idno": "", "account": "", "bank": "", "bic": ""}
		else:
			receiver_fields = {
				"name": cp_name,
				"idno": cp_idno,
				"account": cp_account,
				"bank": cp_bank,
				"bic": cp_bank_bic,
			}
			payer_fields = {"name": "", "idno": "", "account": "", "bank": "", "bic": ""}

	show_payer = True
	show_receiver = True
	if credit_or_debit.startswith("C"):
		show_receiver = False
	elif credit_or_debit.startswith("D"):
		show_payer = False

	if show_payer:
		_append_party(lines, "Payer", **payer_fields)
		_mark_used(
			used,
			details,
			"PayerName",
			"PayerFiscalCode",
			"PayerAccount",
			"PayerBank",
			"PayerBankCode",
		)
	if show_receiver:
		_append_party(lines, "Receiver", **receiver_fields)
		_mark_used(
			used,
			details,
			"BeneficiaryName",
			"BeneficiaryFiscalCode",
			"BeneficiaryAccount",
			"BeneficiaryBank",
			"BeneficiaryBankCode",
		)

	oper_type = (_detail(details, "OperType", "OPERTYPE") or (row.get("oper_type") or "")).strip()
	transaction_code = (
		_detail(details, "TransactionCode", "TRANSACTIONCODE") or (row.get("transaction_code") or "")
	).strip()
	if oper_type:
		_mark_used(used, details, "OperType", "OPERTYPE")
	if transaction_code:
		_mark_used(used, details, "TransactionCode", "TRANSACTIONCODE")
	if oper_type or transaction_code:
		tech_parts = []
		if oper_type:
			tech_parts.append(f"OpType: {oper_type}")
		if transaction_code:
			tech_parts.append(f"TxnCode: {transaction_code}")
		lines.append(" / ".join(tech_parts))

	extras: list[str] = []
	if not show_payer and any(payer_fields.values()):
		_append_party(extras, "Payer", **payer_fields)
		_mark_used(
			used,
			details,
			"PayerName",
			"PayerFiscalCode",
			"PayerAccount",
			"PayerBank",
			"PayerBankCode",
		)
	if not show_receiver and any(receiver_fields.values()):
		_append_party(extras, "Receiver", **receiver_fields)
		_mark_used(
			used,
			details,
			"BeneficiaryName",
			"BeneficiaryFiscalCode",
			"BeneficiaryAccount",
			"BeneficiaryBank",
			"BeneficiaryBankCode",
		)

	if transaction_id:
		extras.append(f"Transaction ID: {transaction_id}")
	if currency:
		extras.append(f"Currency: {currency}")
	if credit_or_debit:
		extras.append(f"Credit/Debit: {credit_or_debit}")

	transfer_type = _detail(details, "TransferType")
	if transfer_type:
		_mark_used(used, details, "TransferType")
		extras.append(f"Transfer Type: {transfer_type}")
	credit_transfer = _detail(details, "CreditTransfer")
	if credit_transfer:
		_mark_used(used, details, "CreditTransfer")
		extras.append(f"Credit Transfer: {credit_transfer}")

	payer_sub = _detail(details, "PayerSubAccount")
	if payer_sub:
		_mark_used(used, details, "PayerSubAccount")
		extras.append(f"Payer SubAccount: {payer_sub}")
	beneficiary_sub = _detail(details, "BeneficiarySubAccount")
	if beneficiary_sub:
		_mark_used(used, details, "BeneficiarySubAccount")
		extras.append(f"Receiver SubAccount: {beneficiary_sub}")

	# Remaining unused transfer-detail tags, in API order.
	for key, value in (details or {}).items():
		if key in used:
			continue
		text = (value or "").strip()
		if not text:
			continue
		extras.append(f"{_label_from_tag(key)}: {text}")

	lines.extend(extras)
	return "\n".join(lines).strip()
