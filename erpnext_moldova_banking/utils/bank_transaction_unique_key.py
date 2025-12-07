import frappe
from frappe import _


def make_transaction_unique_key(company, bank_account, posting_date, deposit, withdrawal, reference_number):
    """Build a deterministic unique key for a bank transaction."""
    # Signed amount: incoming positive, outgoing negative
    amount = (deposit or 0) - (withdrawal or 0)
    posting_date_str = posting_date.isoformat() if posting_date else ""
    ref = (reference_number or "").strip()

    return f"{company}::{bank_account}::{posting_date_str}::{amount:.2f}::{ref}"


def ensure_unique_transaction(doc, method=None):
    """before_insert hook for Bank Transaction.

    Ensures that (company + bank_account + date + amount + reference_number)
    is unique. If a duplicate is found, raises a readable error so that
    Data Import log clearly shows the reason.
    """

    # Make sure company is set (can be derived from Bank Account)
    if not getattr(doc, "company", None) and getattr(doc, "bank_account", None):
        doc.company = frappe.db.get_value("Bank Account", doc.bank_account, "company")

    # Adjust this field name if your DocType uses another name for date
    posting_date = getattr(doc, "date", None) or getattr(doc, "posting_date", None)

    unique_key = make_transaction_unique_key(
        doc.company,
        doc.bank_account,
        posting_date,
        getattr(doc, "deposit", None),
        getattr(doc, "withdrawal", None),
        getattr(doc, "reference_number", None),
    )

    # Save key to technical field
    doc.unique_key = unique_key

    # Check for duplicate before inserting
    if frappe.db.exists("Bank Transaction", {"unique_key": unique_key}):
        amount = (getattr(doc, "deposit", 0) or 0) - (getattr(doc, "withdrawal", 0) or 0)
        ref = (getattr(doc, "reference_number", "") or "").strip() or "-"

        msg = _(
            "Duplicate bank statement line skipped: "
            "Company {0}, Bank Account {1}, Date {2}, Amount {3}, Reference {4}."
        ).format(
            doc.company,
            doc.bank_account,
            posting_date,
            f"{amount:.2f}",
            ref,
        )

        # This exception will be written into Data Import Log for this row
        frappe.throw(msg)
