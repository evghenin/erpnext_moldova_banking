from pydoc import doc
import frappe
from frappe.utils import flt


def normalize_string(value: str) -> str:
    """
    Remove spaces and line breaks, convert to lowercase.
    """
    if not value:
        return ""

    return (
        value.replace(" ", "")
        .replace("\n", "")
        .replace("\r", "")
        .lower()
    )


def handle_bank_transaction(doc, method=None):
    # 1. Load settings
    settings = frappe.get_single("Moldova Banking Settings")

    if not settings.enable_automation:
        return

    # Defensive checks
    if not doc.company or not doc.bank_account or not doc.description:
        return

    normalized_description = normalize_string(doc.description)

    # 2. Iterate over enabled clearing rules
    for rule in settings.automation_rules:
        if rule.disabled:
            continue

        # 3. Match company & bank
        if rule.company != doc.company:
            continue

        ba = frappe.get_doc("Bank Account", doc.bank_account)
        if rule.bank != ba.bank:
            continue

        ba_account = frappe.get_doc("Account", ba.account)
        ar_account = frappe.get_doc("Account", rule.account)

        # Currency guard
        if ba_account.account_currency != ar_account.account_currency:
            continue

        if not rule.description_pattern:
            continue

        # 4. Normalize pattern
        normalized_pattern = normalize_string(rule.description_pattern)
        if not normalized_pattern:
            continue

        # Trim transaction description to pattern length
        candidate = normalized_description[: len(normalized_pattern)]

        # 5. Compare
        if candidate != normalized_pattern:
            continue

        # 6. Match found → create Payment Entry
        create_payment_entry_from_transaction(settings, doc, rule, ba_account, ar_account)

        # One transaction → one rule → one PE
        break


def create_payment_entry_from_transaction(settings, transaction, rule, ba_account, ra_account):
    """
    Create Payment Entry from Bank Transaction using Automation Rule.
    """

    pe = frappe.new_doc("Payment Entry")
    pe.company = transaction.company
    pe.payment_type = "Internal Transfer"
    pe.posting_date = transaction.date
    pe.mode_of_payment = settings.automation_pe_mode_of_payment
    
    pe.reference_no = transaction.reference_number
    pe.reference_date = transaction.date

    if transaction.deposit:
        pe.paid_from = ra_account.name
        pe.paid_from_account_currency = ra_account.account_currency
        pe.paid_to = ba_account.name
        pe.paid_to_account_currency = ba_account.account_currency
        amount = flt(transaction.deposit)
    
    else:
        pe.paid_to = ra_account.name
        pe.paid_to_account_currency = ra_account.account_currency
        pe.paid_from = ba_account.name
        pe.paid_fro_account_currency = ba_account.account_currency
        amount = flt(transaction.withdrawal)
    
    pe.paid_amount = amount
    pe.received_amount = amount

    # Cost Center logic:
    # - Use rule.cost_center if provided
    # - Otherwise fallback to Company default Cost Center
    pe.cost_center = (
        rule.cost_center
        or frappe.get_cached_value("Company", pe.company, "cost_center")
    )

    # Avoid duplicate Payment Entries (best-effort guard)
    if frappe.db.exists(
        "Payment Entry",
        {
            "reference_no": pe.reference_no,
            "reference_date": pe.reference_date,
            "received_amount": pe.received_amount,
            "company": pe.company,
        },
    ):
        return

    pe.insert(ignore_permissions=True)

    # Submit PE safely
    try:
        if settings.automation_pe_submit:
            pe.submit()
    except Exception:
        # Log error but do NOT reconcile
        frappe.log_error(
            frappe.get_traceback(),
            f"Auto-submit Payment Entry failed for Bank Transaction {transaction.name}",
        )
        return

    # Reconcile only if PE is submitted and auto-reconcile enabled
    if (
        settings.automation_pe_submit
        and settings.automation_autoreconcile
        and pe.docstatus == 1
    ):
        reconcile_pe_and_bt(pe, transaction)


def reconcile_pe_and_bt(payment_entry, bank_transaction):
    """
    Reconcile a submitted Payment Entry with a submitted Bank Transaction.

    IMPORTANT:
    In ERPNext v15, reconciliation links are stored on the Bank Transaction itself
    in the child table field `payment_entries` (child doctype: "Bank Transaction Payments").

    This function:
    - Adds a row into Bank Transaction.payment_entries if not already present
    - Updates Bank Transaction.status to "Reconciled" (best-effort)
    """

    # Reload fresh docs to ensure we work with full metadata and latest state
    pe = (
        frappe.get_doc("Payment Entry", payment_entry.name)
        if hasattr(payment_entry, "name")
        else frappe.get_doc("Payment Entry", payment_entry)
    )
    bt = (
        frappe.get_doc("Bank Transaction", bank_transaction.name)
        if hasattr(bank_transaction, "name")
        else frappe.get_doc("Bank Transaction", bank_transaction)
    )

    # Preconditions
    if pe.docstatus != 1:
        return
    if bt.docstatus != 1:
        return

    # Prevent duplicate reconciliation by checking existing child rows
    for row in bt.get("payment_entries") or []:
        # ERPNext stores "Payment Entry" in payment_document; some setups may also store it in payment_type
        if (
            (row.get("payment_document") == "Payment Entry" or row.get("payment_type") == "Payment Entry")
            and row.get("payment_entry") == pe.name
        ):
            return

    allocated_amount = flt(bt.deposit or bt.withdrawal)

    # Append reconciliation row (standard ERPNext v15 fieldnames)
    bt.append(
        "payment_entries",
        {
            "payment_document": "Payment Entry",
            "payment_entry": pe.name,
            "allocated_amount": allocated_amount,
        },
    )

    # Best-effort status update
    try:
        bt.status = "Reconciled"
    except Exception:
        pass

    bt.save(ignore_permissions=True)

    frappe.publish_realtime(
        event="bank_transaction_reload",
        message={
            "doctype": bt.doctype,
            "name": bt.name,
        }
    )
