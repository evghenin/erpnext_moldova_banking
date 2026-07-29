# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

from frappe.utils import flt


def _local(tag: str) -> str:
	if "}" in tag:
		return tag.rsplit("}", 1)[-1]
	return tag


def _child_text(node: ET.Element | None, name: str) -> str:
	if node is None:
		return ""
	for child in list(node):
		if _local(child.tag).lower() == name.lower():
			return (child.text or "").strip()
	return ""


def _parse_yyyymmdd(value: str):
	value = (value or "").strip()
	if not value:
		return None
	try:
		return datetime.strptime(value, "%Y%m%d").date()
	except ValueError:
		try:
			return datetime.strptime(value, "%Y-%m-%d").date()
		except ValueError:
			return None


def parse_statement_xml(xml_text: str) -> list[dict[str, Any]]:
	"""Parse MAIB statement XML into normalized transaction dicts."""
	if not (xml_text or "").strip():
		return []

	try:
		root = ET.fromstring(xml_text)
	except ET.ParseError:
		return []

	rows: list[dict[str, Any]] = []

	for section in list(root):
		section_name = _local(section.tag)
		if section_name.lower() in {"message", "error"}:
			continue

		for account in list(section):
			if _local(account.tag).lower() != "account":
				continue

			currency = _child_text(account, "Currency")
			for date_node in list(account):
				if _local(date_node.tag).lower() != "date":
					continue

				posting_date = _parse_yyyymmdd(date_node.attrib.get("value") or "")
				for txn in list(date_node):
					if _local(txn.tag).lower() != "transaction":
						continue
					row = _parse_transaction(txn, posting_date, currency)
					if row:
						rows.append(row)

	return rows


def _parse_transaction(txn: ET.Element, posting_date, currency: str) -> dict[str, Any] | None:
	document_number = _child_text(txn, "DocumentNumber")
	payment_destination = _child_text(txn, "PaymentDestination")
	credit_or_debit = (_child_text(txn, "CreditOrDebit") or "").strip().upper()
	transaction_id = _child_text(txn, "TransactionId")
	amount = flt(_child_text(txn, "TransactionAmount") or 0)

	contra = None
	for child in list(txn):
		if _local(child.tag).lower() == "contraparty":
			contra = child
			break

	cp_name = ""
	cp_idno = ""
	if contra is not None:
		cp_name = (contra.attrib.get("value") or "").strip()
		cp_idno = _child_text(contra, "FiscalCode")

	if not amount and not transaction_id and not document_number:
		return None

	deposit = 0.0
	withdrawal = 0.0
	if credit_or_debit.startswith("C"):
		deposit = amount
	else:
		# Debit / unknown with amount → withdrawal
		withdrawal = amount

	desc_lines = []
	if payment_destination:
		desc_lines.append(payment_destination)
		desc_lines.append("")
	if amount:
		desc_lines.append(f"Amount: {amount:.2f}")
	if currency:
		desc_lines.append(f"Currency: {currency}")
	if credit_or_debit:
		desc_lines.append(f"Credit/Debit: {credit_or_debit}")
	if document_number:
		desc_lines.append(f"Document Number: {document_number}")
	if transaction_id:
		desc_lines.append(f"Transaction ID: {transaction_id}")
	if cp_name:
		desc_lines.append(f"Counterparty: {cp_name}")
	if cp_idno:
		desc_lines.append(f"Counterparty IDNO: {cp_idno}")

	return {
		"date": posting_date,
		"deposit": deposit,
		"withdrawal": withdrawal,
		"amount": amount,
		"payment_destination": payment_destination,
		"credit_or_debit": credit_or_debit,
		"description": "\n".join(desc_lines).strip(),
		"reference_number": transaction_id or document_number,
		"currency": currency or None,
		"cp_name": cp_name,
		"cp_idno": cp_idno,
		"document_number": document_number,
		"transaction_id": transaction_id,
	}
