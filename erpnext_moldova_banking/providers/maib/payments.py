# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any
from xml.sax.saxutils import escape

import requests

import frappe
from frappe import _

from erpnext_moldova_banking.providers.maib.client import get_access_token, resolve_maib_endpoints

ORDINARY_PATH = "/api/transfers/mdl/ordinary"
STATE_PATH = "/api/transfers/state-queries"

# MAIB API status → app Payment Order.maib_status
STATUS_MAP = {
	"RequiresAction": "Waiting For Authorisation",
	"Waiting For Authorisation": "Waiting For Authorisation",
	"WaitingForAuthorisation": "Waiting For Authorisation",
	"InProcess": "In Process",
	"In Process": "In Process",
	"Processing": "In Process",
	"Succeeded": "Executed",
	"Executed": "Executed",
	"Completed": "Executed",
	"Rejected": "Rejected",
	"Failed": "Rejected",
	"Cancelled": "Rejected",
}


def map_maib_status(raw: str | None) -> str:
	value = (raw or "").strip()
	if not value:
		return "API Error"
	return STATUS_MAP.get(value, STATUS_MAP.get(value.replace(" ", ""), "In Process"))


def _xml_headers(token: str) -> dict[str, str]:
	return {
		"Content-Type": "application/xml",
		"Accept": "application/xml",
		"Authorization": f"Bearer {token}",
	}


def _post_xml(path: str, body: str, settings=None) -> str:
	token_payload = get_access_token(settings)
	token = token_payload["access_token"]
	base = (token_payload.get("_endpoints") or resolve_maib_endpoints(settings))["api_base_url"].rstrip("/")
	if not base:
		frappe.throw(_("MAIB API Base URL is required."))

	url = f"{base}{path}"
	try:
		response = requests.post(url, data=body.encode("utf-8"), headers=_xml_headers(token), timeout=120)
	except requests.RequestException as e:
		frappe.throw(_("Could not reach MAIB payment endpoint: {0}").format(str(e)))

	if response.status_code >= 400:
		err = frappe._dict(
			{
				"http_status": response.status_code,
				"body": response.text or "",
				"path": path,
			}
		)
		frappe.local.maib_last_http_error = err
		frappe.throw(
			_("MAIB payment request failed ({0}): {1}").format(response.status_code, response.text[:800])
		)
	return response.text or ""


def build_ordinary_payment_xml(payload: dict[str, Any]) -> str:
	"""Build Ordinary MDL transfer XML from normalized payload keys."""

	def el(tag: str, value: Any) -> str:
		if value is None or value == "":
			return ""
		return f"<{tag}>{escape(str(value))}</{tag}>"

	parts = [
		'<?xml version="1.0" encoding="UTF-8"?>',
		"<root>",
		el("DOCUMENT_NUMBER", payload.get("document_number")),
		el("PAYMENT_DATE", payload.get("payment_date")),
		el("TRANSACTION_AMOUNT", payload.get("amount")),
		el("INSTRUCTION_TO_BANK", payload.get("details")),
		el("PAYMENT_TYPE", payload.get("payment_type") or "NORMAL"),
		el("SOURCE_ACCOUNT_NUMBER", payload.get("source_account_number")),
		el("SOURCE_PRODUCT_TYPE", payload.get("source_product_type") or "CURRENT_ACCOUNT"),
		el("BENEFICIARY_NAME", payload.get("beneficiary_name")),
		el("BENEFICIARY_FISCAL_CODE", payload.get("beneficiary_fiscal_code")),
		el("DESTINATION_ACCOUNT_NUMBER", payload.get("destination_account_number")),
		el("DESTINATION_BANK_SWIFT_BIC", payload.get("destination_bank_swift_bic")),
		el("BENEFICIARY_RESIDENCE_INDICATOR", payload.get("residency_indicator") or "R"),
	]
	if payload.get("beneficiary_address"):
		parts.append(el("BENEFICIARY_ADDRESS_STREET", payload["beneficiary_address"]))
	parts.append("</root>")
	return "".join(parts)


def create_ordinary_payment(payload: dict[str, Any], settings=None) -> dict[str, str]:
	xml_body = build_ordinary_payment_xml(payload)
	response_text = _post_xml(ORDINARY_PATH, xml_body, settings=settings)
	instruction_id = _parse_instruction_id(response_text)
	if not instruction_id:
		frappe.throw(_("MAIB did not return INSTRUCTION_ID. Response: {0}").format(response_text[:500]))
	return {"instruction_id": instruction_id, "raw": response_text}


def query_instruction_states(instruction_ids: list[str], settings=None) -> list[dict[str, str]]:
	ids = [i for i in instruction_ids if i]
	if not ids:
		return []

	body = ['<?xml version="1.0" encoding="UTF-8"?>', "<root>"]
	for instruction_id in ids:
		body.append(f"<INSTRUCTION_ID>{escape(instruction_id)}</INSTRUCTION_ID>")
	body.append("</root>")

	response_text = _post_xml(STATE_PATH, "".join(body), settings=settings)
	return _parse_state_response(response_text)


def _local(tag: str) -> str:
	return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _parse_instruction_id(xml_text: str) -> str:
	try:
		root = ET.fromstring(xml_text)
	except ET.ParseError:
		return ""
	for node in root.iter():
		if _local(node.tag).upper() == "INSTRUCTION_ID":
			return (node.text or "").strip()
	return ""


def _parse_state_response(xml_text: str) -> list[dict[str, str]]:
	try:
		root = ET.fromstring(xml_text)
	except ET.ParseError:
		return []

	results: list[dict[str, str]] = []
	for node in list(root):
		if _local(node.tag).upper() != "INSTRUCTION":
			continue
		item = {
			"instruction_id": "",
			"status": "",
			"processing_date": "",
			"processing_time": "",
			"comment": "",
		}
		for child in list(node):
			tag = _local(child.tag).upper()
			text = (child.text or "").strip()
			if tag == "INSTRUCTION_ID":
				item["instruction_id"] = text
			elif tag == "INSTRUCTION_STATUS":
				item["status"] = text
			elif tag == "INSTRUCTION_UPDATE_DATE":
				item["processing_date"] = text
			elif tag == "INSTRUCTION_UPDATE_TIME":
				item["processing_time"] = text
			elif tag in {"COMMENTS", "COMMENT", "INSTRUCTION_COMMENT"}:
				item["comment"] = text
		if item["instruction_id"] or item["status"]:
			results.append(item)
	return results
