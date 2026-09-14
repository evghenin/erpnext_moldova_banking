# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

"""Build MAIB INSTRUCTION_TO_BANK from Purchase Invoice / e-Factura / factură."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

import frappe
from frappe.utils import getdate

DETAILS_MAX_LEN = 210

_DASHES = str.maketrans({"–": "-", "—": "-", "−": "-"})
_INSTRUCTION_ALLOWED = re.compile(r"[^A-Za-z0-9,.\-/ ]+")
_MULTI_SPACE = re.compile(r" +")


def clean_instruction_to_bank(text: str) -> str:
	"""Normalize INSTRUCTION_TO_BANK: ASCII letters/digits, comma, dot, dash, slash, spaces."""
	plain = strip_diacritics(text or "").translate(_DASHES)
	cleaned = _INSTRUCTION_ALLOWED.sub("", plain)
	return _MULTI_SPACE.sub(" ", cleaned).strip()[:DETAILS_MAX_LEN]


_DIACRITIC_MAP = str.maketrans(
	{
		"ă": "a",
		"â": "a",
		"î": "i",
		"ș": "s",
		"ş": "s",
		"ț": "t",
		"ţ": "t",
		"Ă": "A",
		"Â": "A",
		"Î": "I",
		"Ș": "S",
		"Ş": "S",
		"Ț": "T",
		"Ţ": "T",
	}
)


def strip_diacritics(text: str) -> str:
	mapped = (text or "").translate(_DIACRITIC_MAP)
	nfd = unicodedata.normalize("NFD", mapped)
	return "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")


def _format_issue_date(value) -> str:
	if not value:
		return ""
	return getdate(value).strftime("%d.%m.%Y")


def _iso_date(value) -> str:
	if not value:
		return ""
	return getdate(value).strftime("%Y-%m-%d")


INSTRUCTION_LANGUAGE = "ro"
INSTRUCTION_RO = {
	"Payment for {types} as per {documents}": "Plata pentru {types} conf. {documents}",
	"{bill_type} no. {bill_no} dated {bill_date}": "{bill_type} nr. {bill_no} din {bill_date}",
	"products": "produse",
	"invoice": "factură",
	"N/N": "f/n",
	"N/D": "f/d",
}


def _t(msgid: str, **kwargs) -> str:
	"""Bank instruction text is always Romanian, independent of UI language."""
	text = INSTRUCTION_RO.get(msgid) or frappe._(msgid, lang=INSTRUCTION_LANGUAGE)
	return text.format(**kwargs) if kwargs else text


def _translated_item_group(item_group: str, lang: str) -> str:
	label = frappe._(item_group, lang=lang) if item_group else ""
	return strip_diacritics((label or item_group or "").strip())


def get_pi_item_group_labels(pi_name: str) -> str:
	rows = frappe.get_all(
		"Purchase Invoice Item",
		filters={"parent": pi_name},
		fields=["item_code", "item_name", "idx"],
		order_by="idx asc",
	)
	seen: list[str] = []
	for row in rows:
		item_group = ""
		if row.get("item_code") and frappe.db.exists("Item", row.item_code):
			item_group = frappe.db.get_value("Item", row.item_code, "item_group") or ""
		label = _translated_item_group(item_group, INSTRUCTION_LANGUAGE)
		if label and label not in seen:
			seen.append(label)
	return ", ".join(seen)


def _doctype_exists(name: str) -> bool:
	return bool(frappe.db.exists("DocType", name))


def _linked_purchase_efactura(pi_name: str) -> dict[str, Any] | None:
	if not _doctype_exists("Purchase eFactura"):
		return None
	name = None
	if frappe.get_meta("Purchase Invoice").has_field("purchase_efactura"):
		name = frappe.db.get_value("Purchase Invoice", pi_name, "purchase_efactura")
	if not name and _doctype_exists("Purchase eFactura Item"):
		name = frappe.db.get_value("Purchase eFactura Item", {"purchase_invoice": pi_name}, "parent")
	if not name:
		return None
	row = frappe.db.get_value(
		"Purchase eFactura",
		name,
		["name", "ef_series", "ef_number", "issue_date", "docstatus"],
		as_dict=True,
	)
	if row and row.get("docstatus") == 2:
		return None
	return row


def _linked_purchase_factura(pi_name: str) -> dict[str, Any] | None:
	if not _doctype_exists("Purchase Factura"):
		return None
	name = frappe.db.get_value("Purchase Factura", {"purchase_invoice": pi_name}, "name")
	if not name:
		return None
	fields = ["name", "issue_date", "docstatus"]
	meta = frappe.get_meta("Purchase Factura")
	if meta.has_field("f_series"):
		fields.extend(["f_series", "f_number"])
	if meta.has_field("ef_series"):
		fields.extend(["ef_series", "ef_number"])
	row = frappe.db.get_value("Purchase Factura", name, fields, as_dict=True)
	if row and row.get("docstatus") == 2:
		return None
	return row


def _factura_ref(series: str | None, number: str | None) -> str:
	return f"{(series or '').strip()}{(number or '').strip()}"


def get_invoice_payment_purpose(pi_name: str) -> dict[str, str] | None:
	pi = frappe.db.get_value(
		"Purchase Invoice",
		pi_name,
		["name", "posting_date", "bill_no", "bill_date"],
		as_dict=True,
	)
	if not pi:
		return None

	product_types = get_pi_item_group_labels(pi_name) or _t("products")
	unknown_no = _t("N/N")
	unknown_date = _t("N/D")

	pef = _linked_purchase_efactura(pi_name)
	if pef:
		bill_type = "e-Factura"
		bill_no = _factura_ref(pef.get("ef_series"), pef.get("ef_number")) or unknown_no
		raw_date = pef.get("issue_date")
		bill_date = _format_issue_date(raw_date) or unknown_date
		bill_date_iso = _iso_date(raw_date)
	else:
		pf = _linked_purchase_factura(pi_name)
		if pf:
			bill_type = _t("invoice")
			series = pf.get("f_series") or pf.get("ef_series")
			number = pf.get("f_number") or pf.get("ef_number")
			bill_no = _factura_ref(series, number) or unknown_no
			raw_date = pf.get("issue_date")
			bill_date = _format_issue_date(raw_date) or unknown_date
			bill_date_iso = _iso_date(raw_date)
		else:
			bill_type = _t("invoice")
			bill_no = (pi.get("bill_no") or "").strip() or unknown_no
			raw_date = pi.get("bill_date")
			bill_date = _format_issue_date(raw_date) or unknown_date
			bill_date_iso = _iso_date(raw_date)

	document = _t(
		"{bill_type} no. {bill_no} dated {bill_date}",
		bill_type=bill_type,
		bill_no=bill_no,
		bill_date=bill_date,
	)
	return {
		"product_types": product_types,
		"bill_type": bill_type,
		"bill_no": bill_no,
		"bill_date": bill_date,
		"document": clean_instruction_to_bank(document),
	}


def build_instruction_to_bank(pi_name: str) -> str:
	"""Build INSTRUCTION_TO_BANK for a Purchase Invoice."""
	return build_instruction_to_bank_for_invoices([pi_name])


def build_instruction_to_bank_for_invoices(
	pi_names: list[str] | None = None,
	documents: list[str] | None = None,
) -> str:
	"""Build INSTRUCTION_TO_BANK from invoices and/or explicit document phrases."""
	names: list[str] = []
	for name in pi_names or []:
		if name and name not in names:
			names.append(name)

	purposes = [get_invoice_payment_purpose(name) for name in names]
	purposes = [p for p in purposes if p]

	product_types: list[str] = []
	for purpose in purposes:
		for label in (purpose["product_types"] or "").split(", "):
			if label and label not in product_types:
				product_types.append(label)
	types_text = ", ".join(product_types) or _t("products")

	if documents is None:
		document_list = [_document_phrase(p) for p in purposes]
	else:
		document_list = [str(d).strip() for d in documents if str(d).strip()]
	document_list = [clean_instruction_to_bank(d) for d in document_list if d]
	if not document_list and not purposes:
		return ""

	text = _t(
		"Payment for {types} as per {documents}",
		types=types_text,
		documents=", ".join(document_list),
	)
	cleaned = clean_instruction_to_bank(text)
	if len(cleaned) <= DETAILS_MAX_LEN:
		return cleaned

	compact = ", ".join(p.get("bill_no") or "" for p in purposes) or ", ".join(document_list)
	return clean_instruction_to_bank(
		_t("Payment for {types} as per {documents}", types=types_text, documents=compact)
	)


def _document_phrase(purpose: dict[str, str]) -> str:
	if purpose.get("document"):
		return purpose["document"]
	return _t(
		"{bill_type} no. {bill_no} dated {bill_date}",
		bill_type=purpose.get("bill_type") or _t("invoice"),
		bill_no=purpose.get("bill_no") or _t("N/N"),
		bill_date=purpose.get("bill_date") or _t("N/D"),
	)
