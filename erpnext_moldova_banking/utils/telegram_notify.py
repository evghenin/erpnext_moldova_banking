# Copyright (c) 2026, Evgheni Nemerenco and contributors
# For license information, please see license.txt

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import requests

import frappe
from frappe import _
from frappe.utils import flt, getdate

SETTINGS_DOCTYPE = "Moldova Banking Settings"
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"
DESC_MAX = 800
TELEGRAM_TEXT_MAX = 4096
API_BATCH_SIZE = 5
BATCH_SEPARATOR = "\n\n────────\n\n"


def _require_system_manager():
	user = frappe.session.user
	if user == "Guest" or "System Manager" not in set(frappe.get_roles(user) or []):
		frappe.throw(_("Not permitted."), frappe.PermissionError)


def _get_settings():
	return frappe.get_single(SETTINGS_DOCTYPE)


def _get_bot_token(settings=None) -> str:
	settings = settings or _get_settings()
	try:
		return settings.get_password("telegram_bot_token") or ""
	except Exception:
		return ""


def send_telegram_message(text: str, settings=None) -> dict[str, Any]:
	"""Send a plain text message to the configured Telegram chat."""
	settings = settings or _get_settings()
	token = _get_bot_token(settings)
	chat_id = (settings.telegram_chat_id or "").strip()
	if not token or not chat_id:
		frappe.throw(_("Telegram Bot Token and Chat ID are required."))

	url = TELEGRAM_API.format(token=token)
	try:
		response = requests.post(
			url,
			json={
				"chat_id": chat_id,
				"text": text,
				"disable_web_page_preview": True,
			},
			timeout=30,
		)
	except requests.RequestException as e:
		frappe.throw(_("Could not reach Telegram API: {0}").format(str(e)))

	payload = {}
	try:
		payload = response.json()
	except Exception:
		payload = {"ok": False, "description": response.text[:500]}

	if response.status_code >= 400 or not payload.get("ok"):
		frappe.throw(
			_("Telegram API error ({0}): {1}").format(
				response.status_code,
				payload.get("description") or response.text[:500],
			)
		)
	return payload


@frappe.whitelist()
def test_telegram_connection() -> dict[str, Any]:
	_require_system_manager()
	settings = _get_settings()
	if not settings.telegram_enabled:
		frappe.throw(_("Enable Telegram Notifications first."))
	send_telegram_message(
		_("ERPNext Moldova Banking: Telegram connection OK."),
		settings=settings,
	)
	return {"ok": True}


def _should_notify(settings, *, source: str, incoming: bool, automation_matched: bool) -> bool:
	if not settings.telegram_enabled:
		return False
	if not _get_bot_token(settings) or not (settings.telegram_chat_id or "").strip():
		return False

	if source == "api" and not settings.telegram_notify_api:
		return False
	if source == "file_import" and not settings.telegram_notify_file_import:
		return False
	if source not in {"api", "file_import"}:
		return False

	if incoming and not settings.telegram_notify_incoming:
		return False
	if not incoming and not settings.telegram_notify_outgoing:
		return False

	if automation_matched and not settings.telegram_notify_automation_matched:
		return False
	if not automation_matched and not settings.telegram_notify_automation_unmatched:
		return False

	return True


def _line_from_description(description: str, prefixes: tuple[str, ...]) -> str:
	for raw in (description or "").splitlines():
		line = raw.strip()
		lower = line.lower()
		for prefix in prefixes:
			if lower.startswith(prefix.lower()):
				return line.split(":", 1)[-1].strip()
	return ""


def _party_name(doc) -> str:
	name = (
		getattr(doc, "bank_party_name", None)
		or getattr(doc, "party_name", None)
		or ""
	)
	name = (name or "").strip()
	if name:
		return name
	desc = doc.description or ""
	return (
		_line_from_description(desc, ("Counterparty:", "Payer:", "Receiver:", "Beneficiary:"))
		or (doc.party or "")
	)


def _party_idno(doc) -> str:
	desc = doc.description or ""
	return _line_from_description(
		desc,
		(
			"Counterparty IDNO:",
			"Payer IDNO:",
			"Receiver IDNO:",
			"Beneficiary IDNO:",
			"PayerFiscalCode:",
			"IDNO:",
			"IDNP:",
		),
	)


def format_bank_transaction_message(
	doc,
	*,
	source: str,
	automation_matched: bool,
	settings=None,
) -> str:
	settings = settings or _get_settings()
	incoming = flt(doc.deposit or 0) > 0
	amount = flt(doc.deposit or 0) or flt(doc.withdrawal or 0)
	currency = (doc.currency or "").strip()

	lines = [
		f"Incoming Bank Transaction {doc.name}"
		if incoming
		else f"Outgoing Bank Transaction {doc.name}"
	]

	if settings.telegram_field_company and doc.company:
		lines.append(f"Company: {doc.company}")
	if settings.telegram_field_date and doc.date:
		lines.append(f"Date: {getdate(doc.date)}")
	if settings.telegram_field_amount and amount:
		sign = "+" if incoming else "-"
		amt = f"{sign}{amount:,.2f}"
		if currency:
			amt = f"{amt} {currency}"
		lines.append(f"Amount: {amt}")
	if settings.telegram_field_party_name:
		party_name = _party_name(doc)
		if party_name:
			role = "Sender" if incoming else "Receiver"
			# For incoming money, counterparty is sender/payer; for outgoing, receiver.
			lines.append(f"{role}: {party_name}")
	if settings.telegram_field_party_idno:
		party_idno = _party_idno(doc)
		if party_idno:
			role = "Sender" if incoming else "Receiver"
			lines.append(f"{role} IDNO/IDNP: {party_idno}")
	if settings.telegram_field_description and doc.description:
		desc = (doc.description or "").strip()
		if len(desc) > DESC_MAX:
			desc = desc[: DESC_MAX - 1] + "…"
		lines.append("")
		lines.append(desc)

	return "\n".join(lines).strip()


def combine_bank_transaction_messages(texts: list[str]) -> str:
	"""Join several BT messages for one Telegram send."""
	parts = [t.strip() for t in texts if (t or "").strip()]
	return BATCH_SEPARATOR.join(parts).strip()


@contextmanager
def telegram_api_batch(*, enabled: bool = True):
	"""Buffer API-imported BT notifications and send them in groups of up to 5."""
	if not enabled:
		yield
		return

	previous_flag = getattr(frappe.flags, "moldova_telegram_batching", False)
	previous_queue = getattr(frappe.flags, "moldova_telegram_batch", None)
	frappe.flags.moldova_telegram_batching = True
	frappe.flags.moldova_telegram_batch = []
	try:
		yield
		_flush_telegram_batch()
	finally:
		frappe.flags.moldova_telegram_batching = previous_flag
		frappe.flags.moldova_telegram_batch = previous_queue


def _flush_telegram_batch(settings=None) -> int:
	"""Send queued texts in chunks of at most API_BATCH_SIZE (and Telegram length)."""
	queue = getattr(frappe.flags, "moldova_telegram_batch", None)
	if not queue:
		return 0

	sent = 0
	settings = settings or _get_settings()
	while queue:
		n = min(API_BATCH_SIZE, len(queue))
		while n > 1 and len(combine_bank_transaction_messages(queue[:n])) > TELEGRAM_TEXT_MAX:
			n -= 1
		chunk = [queue.pop(0) for _ in range(n)]
		text = combine_bank_transaction_messages(chunk)
		if len(text) > TELEGRAM_TEXT_MAX:
			text = text[: TELEGRAM_TEXT_MAX - 1] + "…"
		try:
			send_telegram_message(text, settings=settings)
			sent += 1
		except Exception:
			frappe.log_error(
				frappe.get_traceback(),
				"Telegram notify batch failed",
			)
	return sent


def notify_new_bank_transaction(
	doc,
	*,
	source: str,
	automation_matched: bool = False,
	settings=None,
) -> bool:
	"""Send one Telegram message for a new BT when settings filters allow it.

	Returns True if a message was sent. Never raises into callers.
	"""
	try:
		settings = settings or _get_settings()
		incoming = flt(doc.deposit or 0) > 0
		# Zero-amount edge: treat as outgoing filter gate (must have outgoing enabled)
		if flt(doc.deposit or 0) <= 0 and flt(doc.withdrawal or 0) <= 0:
			incoming = False

		if not _should_notify(
			settings,
			source=source,
			incoming=incoming,
			automation_matched=bool(automation_matched),
		):
			return False

		text = format_bank_transaction_message(
			doc,
			source=source,
			automation_matched=bool(automation_matched),
			settings=settings,
		)
		queue = getattr(frappe.flags, "moldova_telegram_batch", None)
		if getattr(frappe.flags, "moldova_telegram_batching", False) and queue is not None:
			queue.append(text)
			if len(queue) >= API_BATCH_SIZE:
				_flush_telegram_batch(settings=settings)
			return True

		send_telegram_message(text, settings=settings)
		return True
	except Exception:
		frappe.log_error(
			frappe.get_traceback(),
			f"Telegram notify failed for Bank Transaction {getattr(doc, 'name', '')}",
		)
		return False


def on_bank_transaction_submit(doc, method=None):
	"""Doc event: notify after automation hook has set the match flag."""
	source = getattr(frappe.flags, "moldova_bt_source", None)
	if not source:
		return
	matched = bool(getattr(frappe.flags, "moldova_bt_automation_matched", False))
	notify_new_bank_transaction(doc, source=source, automation_matched=matched)
