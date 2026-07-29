# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Build Bank Transaction.description for MAIB statement / transfer details.

Aligned with DBO import format:
  ground
  <blank>
  Amount / Document Number / Date Written
  Payer / Receiver blocks (name, IDNO, account, bank, BIC)
"""

from __future__ import annotations

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


def build_maib_transaction_description(
	row: dict[str, Any] | None = None,
	*,
	transfer_details: dict[str, str] | None = None,
	company_iban: str = "",
	company_name: str = "",
	company_idno: str = "",
) -> str:
	"""Compose expanded description from statement row and optional Transfer Details."""
	row = row or {}
	details = transfer_details or row.get("transfer_details") or {}

	payment_destination = (
		_detail(details, "PaymentDestination")
		or (row.get("payment_destination") or "")
		or ""
	).strip()
	# Legacy descriptions may start with destination as first paragraph.
	if not payment_destination:
		raw = (row.get("description") or "").strip()
		if raw and "\nAmount:" not in raw and not raw.startswith("Amount:"):
			payment_destination = raw.split("\n\n", 1)[0].strip()

	amount = row.get("amount")
	if amount is None:
		amount = flt(row.get("deposit") or 0) or flt(row.get("withdrawal") or 0)
	if not amount:
		amount = flt(_detail(details, "PayerAmount") or 0)

	document_number = (
		_detail(details, "DocumentNumber") or (row.get("document_number") or "")
	).strip()
	transaction_id = (row.get("transaction_id") or row.get("reference_number") or "").strip()
	currency = (_detail(details, "Currency") or (row.get("currency") or "")).strip()

	date_written = _detail(details, "Date")
	if not date_written and row.get("date"):
		try:
			date_written = getdate(row.get("date")).strftime("%Y-%m-%d")
		except Exception:
			date_written = str(row.get("date"))

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

	# Prefer full Transfer Details parties when present.
	payer_name = _detail(details, "PayerName")
	beneficiary_name = _detail(details, "BeneficiaryName")
	if payer_name or beneficiary_name:
		_append_party(
			lines,
			"Payer",
			name=payer_name,
			idno=_detail(details, "PayerFiscalCode"),
			account=_detail(details, "PayerAccount", "PayerSubAccount"),
			bank=_detail(details, "PayerBank"),
			bic=_detail(details, "PayerBankCode"),
		)
		_append_party(
			lines,
			"Receiver",
			name=beneficiary_name,
			idno=_detail(details, "BeneficiaryFiscalCode"),
			account=_detail(details, "BeneficiaryAccount", "BeneficiarySubAccount"),
			bank=_detail(details, "BeneficiaryBank"),
			bic=_detail(details, "BeneficiaryBankCode"),
		)
	else:
		# Statement-only: counterparty + our company account when known.
		cp_name = (row.get("cp_name") or "").strip()
		cp_idno = (row.get("cp_idno") or "").strip()
		cp_account = (row.get("cp_account") or "").strip()
		cp_bank = (row.get("cp_bank") or "").strip()
		cp_bank_bic = (row.get("cp_bank_bic") or "").strip()

		our_name = (company_name or "").strip()
		our_idno = (company_idno or "").strip()
		our_account = (company_iban or "").strip()

		if credit_or_debit.startswith("C"):
			# Incoming: counterparty paid us.
			_append_party(
				lines,
				"Payer",
				name=cp_name,
				idno=cp_idno,
				account=cp_account,
				bank=cp_bank,
				bic=cp_bank_bic,
			)
			_append_party(
				lines,
				"Receiver",
				name=our_name,
				idno=our_idno,
				account=our_account,
			)
		else:
			# Outgoing / unknown: we paid counterparty.
			_append_party(
				lines,
				"Payer",
				name=our_name,
				idno=our_idno,
				account=our_account,
			)
			_append_party(
				lines,
				"Receiver",
				name=cp_name,
				idno=cp_idno,
				account=cp_account,
				bank=cp_bank,
				bic=cp_bank_bic,
			)

	if currency:
		lines.append(f"Currency: {currency}")
	if transaction_id:
		lines.append(f"Transaction ID: {transaction_id}")
	transfer_type = _detail(details, "TransferType", "CreditTransfer")
	if transfer_type:
		lines.append(f"Transfer Type: {transfer_type}")

	return "\n".join(lines).strip()
