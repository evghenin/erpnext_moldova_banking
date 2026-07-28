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

        if rule.document_type == "Journal Entry" and rule.second_account:
            second_account = frappe.get_doc("Account", rule.second_account)

        # Currency guard
        if second_account and ba_account.account_currency != second_account.account_currency:
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
        if rule.document_type == "Payment Entry":
            create_payment_entry_from_transaction(settings, doc, rule, ba_account)
        elif rule.document_type == "Journal Entry":
            create_journal_entry_from_transaction(settings, doc, rule, ba_account, second_account)

        # One transaction → one rule → one PE or JE
        break


def create_payment_entry_from_transaction(settings, transaction, rule, ba_account):
    """
    Create Payment Entry from Bank Transaction using Automation Rule.
    """

    pe = frappe.new_doc("Payment Entry")
    pe.company = transaction.company
    pe.payment_type = "Internal Transfer"
    pe.posting_date = transaction.date
    pe.mode_of_payment = settings.automation_mode_of_payment
    
    pe.reference_no = transaction.reference_number
    pe.reference_date = transaction.date

    if transaction.party_type and transaction.party:
        pe.party_type = transaction.party_type
        pe.party = transaction.party

    if transaction.deposit:
        pe.paid_to = ba_account.name
        pe.paid_to_account_currency = ba_account.account_currency
        amount = flt(transaction.deposit)
    
    else:
        pe.paid_from = ba_account.name
        pe.paid_from_account_currency = ba_account.account_currency
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

    # Create PE safely
    try:
        pe.insert(ignore_permissions=True)
        saved = True
    except Exception:
        # Log error
        frappe.log_error(
            frappe.get_traceback(),
            f"Auto-create Payment Entry failed for Bank Transaction {transaction.name}",
        )
        return

    # Submit PE safely
    try:
        if saved and settings.automation_submit:
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
        settings.automation_submit
        and settings.automation_autoreconcile
        and pe.docstatus == 1
    ):
        reconcile_pe_and_bt(pe, transaction)


def create_journal_entry_from_transaction(settings, transaction, rule, ba_account, second_account):
    """
    Create Journal Entry from Bank Transaction using Automation Rule.
    """

    je = frappe.new_doc("Journal Entry")
    je.company = transaction.company
    je.voucher_type = rule.journal_entry_type
    je.posting_date = transaction.date
    je.cheque_no = transaction.reference_number
    je.cheque_date = transaction.date
    je.mode_of_payment = settings.automation_mode_of_payment

    amount = flt(transaction.deposit or transaction.withdrawal)
    company = frappe.get_doc("Company", transaction.company)

    # Debit / Credit lines
    if transaction.deposit:
        # Debit: Bank Account
        je.append(
            "accounts",
            {
                "account": ba_account.name,
                "debit_in_account_currency": amount,
                "account_currency": ba_account.account_currency,
                "cost_center": rule.cost_center,
            },
        )
        # Credit: Second Account
        row = {
                "account": second_account.name,
                "credit_in_account_currency": amount,
                "account_currency": second_account.account_currency,
                "cost_center": rule.cost_center or company.cost_center,
            }
        
        if je.voucher_type == "Bank Entry" and second_account.account_type in ("Receivable", "Payable"):
            if transaction.party_type and transaction.party:

                row["party_type"] = transaction.party_type
                row["party"] = transaction.party

        je.append(
            "accounts",
            row,
        )
    else:
        # Credit: Bank Account
        je.append(
            "accounts",
            {
                "account": ba_account.name,
                "credit_in_account_currency": amount,
                "account_currency": ba_account.account_currency,
            },
        )
        # Debit: Second Account
        row = {
                "account": second_account.name,
                "debit_in_account_currency": amount,
                "account_currency": second_account.account_currency,
                "cost_center": rule.cost_center or company.cost_center,
            }
        
        # if je.voucher_type == "Bank Entry" and transaction.party_type and transaction.party:
        #     row.party_type = transaction.party_type
        #     row.party = transaction.party

        je.append(
            "accounts",
            row,
        )

    # Create JE safely
    try:
        je.insert(ignore_permissions=True)
        saved = True
    except Exception:
        # Log error
        frappe.log_error(
            frappe.get_traceback(),
            f"Auto-create Journal Entry failed for Bank Transaction {transaction.name}",
        )
        return

    # Submit JE safely
    try:
        if saved and settings.automation_submit:
            je.submit()
    except Exception:
        # Log error but do NOT reconcile
        frappe.log_error(
            frappe.get_traceback(),
            f"Auto-submit Journal Entry failed for Bank Transaction {transaction.name}",
        )
        return
    
    # Reconcile only if JE is submitted and auto-reconcile enabled
    if (
        settings.automation_submit
        and settings.automation_autoreconcile
        and je.docstatus == 1
    ):
        reconcile_je_and_bt(je, transaction)


def reconcile_pe_and_bt(payment_entry, bank_transaction):
    """Reconcile a submitted Payment Entry with a submitted Bank Transaction."""
    pe_name = payment_entry.name if hasattr(payment_entry, "name") else payment_entry
    reconcile_voucher_and_bt("Payment Entry", pe_name, bank_transaction)


def reconcile_je_and_bt(journal_entry, bank_transaction):
    """Reconcile a submitted Journal Entry with a submitted Bank Transaction."""
    je_name = journal_entry.name if hasattr(journal_entry, "name") else journal_entry
    reconcile_voucher_and_bt("Journal Entry", je_name, bank_transaction)


def reconcile_voucher_and_bt(voucher_doctype, voucher_name, bank_transaction):
    """
    Reconcile a submitted voucher with a submitted Bank Transaction via the
    standard ERPNext v15 path (same as Bank Reconciliation Tool.reconcile_vouchers):

    - BankTransaction.add_payment_entries (allocated_amount=0 placeholder)
    - save() → before_update_after_submit → allocate_payment_entries / set_status

    Links are stored in Bank Transaction.payment_entries
    (child doctype: Bank Transaction Payments) for both PE and JE.
    """
    bt_name = (
        bank_transaction.name
        if hasattr(bank_transaction, "name")
        else bank_transaction
    )

    if frappe.db.get_value(voucher_doctype, voucher_name, "docstatus") != 1:
        return

    bt = frappe.get_doc("Bank Transaction", bt_name)
    if bt.docstatus != 1:
        return

    if flt(bt.unallocated_amount) <= 0:
        return

    for row in bt.get("payment_entries") or []:
        if row.get("payment_document") == voucher_doctype and row.get("payment_entry") == voucher_name:
            return

    try:
        # Matches erpnext.accounts.doctype.bank_reconciliation_tool
        # .bank_reconciliation_tool.reconcile_vouchers
        bt.add_payment_entries(
            [
                {
                    "payment_doctype": voucher_doctype,
                    "payment_name": voucher_name,
                }
            ]
        )
        bt.save(ignore_permissions=True)
    except Exception:
        frappe.log_error(
            frappe.get_traceback(),
            f"Auto-reconcile {voucher_doctype} {voucher_name} failed for Bank Transaction {bt_name}",
        )
        return

    frappe.publish_realtime(
        event="bank_transaction_reload",
        message={
            "doctype": bt.doctype,
            "name": bt.name,
        },
    )