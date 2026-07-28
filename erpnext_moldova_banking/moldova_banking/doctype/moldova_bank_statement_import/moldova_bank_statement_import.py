# Copyright (c) 2019, Frappe Technologies and contributors
# For license information, please see license.txt


import csv
import io
import json
import re
from datetime import datetime
from typing import TYPE_CHECKING

import frappe
import openpyxl
from frappe import _
from frappe.core.doctype.data_import.data_import import DataImport
from frappe.core.doctype.data_import.importer import ImportFile, Importer
from frappe.utils.background_jobs import enqueue, is_job_enqueued
from frappe.utils.file_manager import get_file, save_file
from frappe.utils.xlsxutils import ILLEGAL_CHARACTERS_RE, handle_html
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter

MOLDOVA_BANK_STATEMENT_IMPORT = "Moldova Bank Statement Import"
INVALID_VALUES = ("", None)


class MoldovaBankStatementImport(DataImport):
    # begin: auto-generated types
    # This code is auto-generated. Do not modify anything in this block.

    if TYPE_CHECKING:
        from frappe.types import DF

        bank: DF.Link | None
        bank_account: DF.Link
        company: DF.Link
        custom_delimiters: DF.Check
        delimiter_options: DF.Data | None
        google_sheets_url: DF.Data | None
        import_file: DF.Attach | None
        import_dbo_fromat: DF.Check
        import_type: DF.Literal["", "Insert New Records", "Update Existing Records"]
        mute_emails: DF.Check
        reference_doctype: DF.Link
        show_failed_logs: DF.Check
        status: DF.Literal["Pending", "Success", "Partial Success", "Error"]
        submit_after_import: DF.Check
        template_options: DF.Code | None
        template_warnings: DF.Code | None
    # end: auto-generated types

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def validate(self):
        doc_before_save = self.get_doc_before_save()
        if (
            not (self.import_file or self.google_sheets_url)
            or (doc_before_save and doc_before_save.import_file != self.import_file)
            or (doc_before_save and doc_before_save.google_sheets_url != self.google_sheets_url)
        ):
            template_options_dict = {}
            column_to_field_map = {}
            bank = frappe.get_doc("Bank", self.bank)
            for i in bank.bank_transaction_mapping:
                column_to_field_map[i.file_field] = i.bank_transaction_field
            template_options_dict["column_to_field_map"] = column_to_field_map
            self.template_options = json.dumps(template_options_dict)

            self.template_warnings = ""

        if self.import_file and not self.import_file.lower().endswith(".txt"):
            self.validate_import_file()
            self.validate_google_sheets_url()

    def start_import(self):
        preview = frappe.get_doc(
            MOLDOVA_BANK_STATEMENT_IMPORT, self.name
        ).get_preview_from_template(self.import_file, self.google_sheets_url)

        ba = frappe.get_doc("Bank Account", self.bank_account)
        if not ba or not ba.iban:
            frappe.throw(_("Please fill the Bank Account's Iban field"))

        if "Bank Account" not in json.dumps(preview["columns"]):
            frappe.throw(_("Please add the Bank Account column"))

        from frappe.utils.scheduler import is_scheduler_inactive

        if is_scheduler_inactive() and not frappe.flags.in_test:
            frappe.throw(
                _("Scheduler is inactive. Cannot import data."), title=_("Scheduler Inactive")
            )

        job_id = f"moldova_bank_statement_import::{self.name}"
        if not is_job_enqueued(job_id):
            enqueue(
                start_import,
                queue="default",
                timeout=6000,
                event="data_import",
                job_id=job_id,
                data_import=self.name,
                bank_account=self.bank_account,
                import_file_path=self.import_file,
                google_sheets_url=self.google_sheets_url,
                bank=self.bank,
                template_options=self.template_options,
                now=frappe.conf.developer_mode or frappe.flags.in_test,
            )
            return True

        return False

@frappe.whitelist()
def convert_dbo_to_csv(data_import, dbo_file_path):
    from frappe.utils import cstr

    doc = frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, data_import)

    _file_doc, content = get_file(dbo_file_path)

    is_dbo = is_dbo_format(content)
    if not is_dbo:
        frappe.throw(_("The uploaded file does not appear to be in valid DBO format."))

    if is_dbo and not doc.import_dbo_fromat:
        frappe.throw(_("DBO file detected. Please enable 'Import DBO Format' to proceed."))

    transactions = parse_dbo(content, doc)

    if not transactions:
        frappe.throw(_("Parsed file is not in valid DBO format or contains no transactions."))

    # Use in-memory file buffer instead of writing to temp file
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)

    headers = [
        "Date",
        "Deposit",
        "Withdrawal",
        "Description",
        "Reference Number",
        "Bank Account",
        "Currency",
        "Party Type",
        "Party",
        "Party Name/Account Holder (Bank Statement)",
        "Party Account No. (Bank Statement)",
        "Party IBAN (Bank Statement)",
    ]

    writer.writerow(headers)

    for txn in transactions:

        # Date & currency safe formatting
        dt = txn.get("date")
        date_str = cstr(dt) if dt else ""

        deposit = txn.get("deposit") or 0
        withdrawal = txn.get("withdrawal") or 0
        description = (txn.get("description") or "").replace("\r\n", "\n") or ""
        reference = txn.get("reference_number") or ""
        currency = txn.get("currency", "")

        # Party resolution based on IDNO
        party_type, party = resolve_party_by_idno(txn)

        # Counterparty account split into account number vs IBAN
        cp_account_raw = (txn.get("cp_account") or "").strip()
        is_iban = is_iban_valid(cp_account_raw.replace(" ", ""))

        party_iban = cp_account_raw if is_iban else ""
        party_account_no = "" if is_iban else cp_account_raw

        writer.writerow([
            date_str,
            deposit,
            withdrawal,
            description,
            reference,
            doc.bank_account,
            currency,
            party_type or "",
            party or "",
            (txn.get("cp_name") or ""),
            party_account_no,
            party_iban,
        ])

    # Prepare in-memory CSV for upload
    csv_content = csv_buffer.getvalue().encode("utf-8")
    csv_buffer.close()

    filename = f"{frappe.utils.now_datetime().strftime('%Y%m%d%H%M%S')}_converted_dbo.csv"

    # Save to File Manager
    saved_file = save_file(
        filename, csv_content, doc.doctype, doc.name, is_private=True, df="import_file"
    )

    return saved_file.file_url


@frappe.whitelist()
def get_preview_from_template(data_import, import_file=None, google_sheets_url=None):
    return frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, data_import).get_preview_from_template(
        import_file, google_sheets_url
    )


@frappe.whitelist()
def form_start_import(data_import):
    job_id = frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, data_import).start_import()
    return job_id is not None


@frappe.whitelist()
def download_errored_template(data_import_name):
    data_import = frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, data_import_name)
    data_import.export_errored_rows()


@frappe.whitelist()
def download_import_log(data_import_name):
    return frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, data_import_name).download_import_log()


def is_dbo_format(content: str) -> bool:
    """Check if the content has key DBO tags"""
    required_tags = ["DocStart", "DocEnd", "BEGINDATE", "ENDDATE"]
    return all(tag in content for tag in required_tags)

def has_account_info(content: str) -> bool:
    """Check if the content has key DBO tags"""
    required_tags = ["SECTIONACCOUNTSTART", "SECTIONACCOUNTSTOP"]
    return all(tag in content for tag in required_tags)

def parse_data_from_template(raw_data):
    data = []

    for _i, row in enumerate(raw_data):
        if all(v in INVALID_VALUES for v in row):
            # empty row
            continue

        data.append(row)

    return data


def start_import(
    data_import, bank_account, import_file_path, google_sheets_url, bank, template_options
):
    """This method runs in background job"""

    update_mapping_db(bank, template_options)

    data_import = frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, data_import)
    file = import_file_path if import_file_path else google_sheets_url

    import_file = ImportFile("Bank Transaction", file=file, import_type="Insert New Records")

    data = parse_data_from_template(import_file.raw_data)
    # Importer expects 'Data Import' class, which has 'payload_count' attribute
    if not data_import.get("payload_count"):
        data_import.payload_count = len(data) - 1

    if import_file_path:
        add_bank_account(data, bank_account)
        write_files(import_file, data)

    try:
        i = Importer(data_import.reference_doctype, data_import=data_import)
        i.import_data()
    except Exception:
        frappe.db.rollback()
        data_import.db_set("status", "Error")
        data_import.log_error("Moldova Bank Statement Import failed")
    finally:
        frappe.flags.in_import = False

    frappe.publish_realtime("data_import_refresh", {"data_import": data_import.name})


def update_mapping_db(bank, template_options):
    """Persist import column mapping to Bank only when it actually changed.

    Avoids destructive wipe when template_options is missing/empty/unchanged.
    """
    if not template_options:
        return

    try:
        options = (
            json.loads(template_options)
            if isinstance(template_options, str)
            else template_options
        )
    except (TypeError, ValueError, json.JSONDecodeError):
        return

    new_map = options.get("column_to_field_map") or {}
    if not isinstance(new_map, dict) or not new_map:
        return

    # Drop incomplete pairs so we never persist blank mappings
    new_map = {
        str(file_field): bank_transaction_field
        for file_field, bank_transaction_field in new_map.items()
        if file_field not in (None, "") and bank_transaction_field not in (None, "")
    }
    if not new_map:
        return

    bank = frappe.get_doc("Bank", bank)
    current_map = {
        str(row.file_field): row.bank_transaction_field
        for row in (bank.bank_transaction_mapping or [])
        if row.file_field not in (None, "") and row.bank_transaction_field not in (None, "")
    }

    if current_map == new_map:
        return

    bank.set("bank_transaction_mapping", [])
    for file_field, bank_transaction_field in new_map.items():
        bank.append(
            "bank_transaction_mapping",
            {
                "bank_transaction_field": bank_transaction_field,
                "file_field": file_field,
            },
        )

    bank.save()


def add_bank_account(data, bank_account):
    """Add bank account information to data rows."""
    bank_account_loc = None
    if "Bank Account" not in data[0]:
        data[0].append("Bank Account")
    else:
        for loc, header in enumerate(data[0]):
            if header == "Bank Account":
                bank_account_loc = loc

    for row in data[1:]:
        if bank_account_loc:
            row[bank_account_loc] = bank_account
        else:
            row.append(bank_account)


def write_files(import_file, data):
    """Write processed data to CSV or Excel files."""
    full_file_path = import_file.file_doc.get_full_path()
    parts = import_file.file_doc.get_extension()
    extension = parts[1]
    extension = extension.lstrip(".")

    if extension == "csv":
        with open(full_file_path, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)
    elif extension in {"xlsx", "xls"}:
        write_xlsx(data, "trans", file_path=full_file_path)


def write_xlsx(data, sheet_name, wb=None, column_widths=None, file_path=None):
    """Write data to Excel file with formatting."""
    # from xlsx utils with changes
    column_widths = column_widths or []
    if wb is None:
        wb = openpyxl.Workbook(write_only=True)

    ws = wb.create_sheet(sheet_name, 0)

    for i, column_width in enumerate(column_widths):
        if column_width:
            ws.column_dimensions[get_column_letter(i + 1)].width = column_width

    row1 = ws.row_dimensions[1]
    row1.font = Font(name="Calibri", bold=True)

    clean_row = []
    for row in data:

        for item in row:
            if isinstance(item, str) and (
                sheet_name not in ["Data Import Template", "Data Export"]
            ):
                value = handle_html(item)
            else:
                value = item

            if isinstance(item, str) and next(ILLEGAL_CHARACTERS_RE.finditer(value), None):
                # Remove illegal characters from the string
                value = re.sub(ILLEGAL_CHARACTERS_RE, "", value)

            clean_row.append(value)

        ws.append(clean_row)

    wb.save(file_path)
    return True


@frappe.whitelist()
def get_import_status(docname):
    import_status = {}

    data_import = frappe.get_doc(MOLDOVA_BANK_STATEMENT_IMPORT, docname)
    import_status["status"] = data_import.status

    logs = frappe.get_all(
        "Data Import Log",
        fields=["count(*) as count", "success"],
        filters={"data_import": docname},
        group_by="success",
    )

    total_payload_count = 0

    for log in logs:
        total_payload_count += log.get("count", 0)
        if log.get("success"):
            import_status["success"] = log.get("count")
        else:
            import_status["failed"] = log.get("count")

    import_status["total_records"] = total_payload_count

    return import_status


@frappe.whitelist()
def get_import_logs(docname: str):
    frappe.has_permission(MOLDOVA_BANK_STATEMENT_IMPORT, throw=True)

    return frappe.get_all(
        "Data Import Log",
        fields=["success", "docname", "messages", "exception", "row_indexes"],
        filters={"data_import": docname},
        limit_page_length=5000,
        order_by="log_index",
    )


@frappe.whitelist()
def upload_bank_statement(**args):
    args = frappe._dict(args)
    bsi = frappe.new_doc(MOLDOVA_BANK_STATEMENT_IMPORT)

    if args.company:
        bsi.update({
            "company": args.company,
        })

    if args.bank_account:
        bsi.update({"bank_account": args.bank_account})

    return bsi

def parse_date(value: str):
    """Parse date strictly in DD.MM.YYYY format."""
    value = (value or "").strip()
    if not value:
        return None

    try:
        return datetime.strptime(value, "%d.%m.%Y").date()
    except ValueError:
        # fallback — на случай если банк вдруг прислал ISO
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            frappe.log_error(
                title="Invalid date format in DBO import",
                message=f"Unrecognized date format: {value}",
            )
            return None

def parse_dbo(content: str, import_doc):
    """Parse DBO formatted bank statement content into transactions."""
    # This is a placeholder implementation. The actual parsing logic will depend on the DBO format specification.
    # For demonstration, let's assume we have a simple parser that extracts transactions based on known tags.

    from frappe.utils import flt

    ba_doc = frappe.get_doc("Bank Account", import_doc.bank_account)
    a_doc = frappe.get_doc("Account", ba_doc.account)

    lines = [ln.strip() for ln in content.splitlines() if ln.strip()]

    header = {}
    docs = []
    current_doc = None
    in_account_section = False

    for line in lines:
        if line == "SECTIONACCOUNTSTART":
            in_account_section = True
            continue

        if line == "SECTIONACCOUNTSTOP":
            in_account_section = False
            continue

        if line == "DocStart":
            current_doc = {}
            continue

        if line == "DocEnd":
            if current_doc:
                docs.append(current_doc)
                current_doc = None
            continue

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip().upper()
        value = value.strip()
        if value == "null":
            value = None

        if in_account_section:
            header[key] = value

        elif current_doc is not None:
            current_doc[key] = value

        else:
            header[key] = value

    account_iban = header.get("ACCOUNT")
    opening_balance = flt(header.get("STARTREST") or 0)
    closing_balance_bank = flt(header.get("STOPREST") or 0)
    begin_date_str = header.get("BEGINDATE")
    end_date_str = header.get("ENDDATE")
    currency_code = (header.get("CURRCODE") or "").strip() or None

    
    if account_iban and account_iban != ba_doc.iban:
        frappe.throw(_("Bank Account Iban {0} is not equal to Iban from DBO file {1}").format(ba_doc.iban, account_iban))

    if currency_code and currency_code != a_doc.account_currency:
        frappe.throw(_("Account Currency {0} is not equal to Currency from DBO file {1}").format(a_doc.account_currency, currency_code))

    if not account_iban:
        account_iban = ba_doc.iban

    if not currency_code:
        currency_code = a_doc.account_currency

    transactions: list[dict] = []
    running_balance = opening_balance
    from_date = None
    to_date = None

    for doc in docs:
        desc_lines = []
        # Raw fields from statement
        document_number = (doc.get("DOCUMENTNUMBER") or "").strip()
        document_date_str = (doc.get("DOCUMENTDATE") or "").strip()
        date_written_str = (doc.get("DATEWRITTEN") or "").strip()

        posting_date = parse_date(document_date_str or date_written_str)

        amount = flt(doc.get("AMOUNT") or 0)

        payer_account = (doc.get("PAYERACCOUNT") or "").strip()
        receiver_account = (doc.get("RECEIVERACCOUNT") or "").strip()
        
        if account_iban not in [payer_account, receiver_account]:        
            frappe.throw(_("DBO file contains transactions which are not belongs to the Bank Account"))

        payer_name = (doc.get("PAYER") or "").strip()
        receiver_name = (doc.get("RECEIVER") or "").strip()

        payer_fcode = (doc.get("PAYERFCODE") or "").strip()
        receiver_fcode = (doc.get("RECEIVERFCODE") or "").strip()

        payer_bank = (doc.get("PAYERBANK") or "").strip()
        receiver_bank = (doc.get("RECEIVERBANK") or "").strip()

        payer_bank_bic = (doc.get("PAYERBANKBIC") or "").strip()
        receiver_bank_bic = (doc.get("RECEIVERBANKBIC") or "").strip()

        oper_type = (doc.get("OPERTYPE") or "").strip()
        transaction_code = (doc.get("TRANSACTIONCODE") or "").strip()

        base_ground = (doc.get("GROUND") or "").strip()

        deposit = 0.0
        withdrawal = 0.0
        cp_role = ""
        cp_name = ""
        cp_account = ""
        cp_idno = ""
        cp_bank = ""
        cp_bank_bic = ""

        # Direction: if our account is payer -> withdrawal, if receiver -> deposit
        if account_iban and payer_account == account_iban and amount:
            withdrawal = amount
            running_balance -= amount
            cp_role = "Receiver"
            cp_name = receiver_name
            cp_account = receiver_account
            cp_idno = receiver_fcode
            cp_bank = receiver_bank
            cp_bank_bic = receiver_bank_bic

        elif account_iban and receiver_account == account_iban and amount:
            deposit = amount
            running_balance += amount
            cp_role = "Payer"
            cp_name = payer_name
            cp_account = payer_account
            cp_idno = payer_fcode
            cp_bank = payer_bank
            cp_bank_bic = payer_bank_bic

        # Track min/max posting date
        if posting_date:
            if not from_date or posting_date < from_date:
                from_date = posting_date
            if not to_date or posting_date > to_date:
                to_date = posting_date

        # Build description in required format:
        # 1) GROUND
        # 2) empty line
        # 3) "Amount: ..."
        # 4) "Document Number: ..."
        # 5) "Date Written: ..."
        # 6) "<Receiver/Payer>: ..."
        # 7) "<Receiver/Payer> IDNO: ..."
        # 8) "<Receiver/Payer> Account: ..."
        # 9) "<Receiver/Payer> Bank: ..."
        # 10) "<Receiver/Payer> Bank BIC: ..."

        if base_ground:
            desc_lines.append(base_ground)

        # Empty line
        if desc_lines:
            desc_lines.append("")

        # Amount
        if amount:
            desc_lines.append(f"Amount: {amount:.2f}")

        # Document number
        if document_number:
            desc_lines.append(f"Document Number: {document_number}")

        # Date written (string as in statement)
        if date_written_str:
            desc_lines.append(f"Date Written: {date_written_str}")

        # Counterparty block
        if cp_role and (cp_name or cp_account or cp_idno or cp_bank or cp_bank_bic):
            # Name
            if cp_name:
                desc_lines.append(f"{cp_role}: {cp_name}")
            # IDNO
            if cp_idno:
                desc_lines.append(f"{cp_role} IDNO: {cp_idno}")
            # Account
            if cp_account:
                desc_lines.append(f"{cp_role} Account: {cp_account}")
            # Bank
            if cp_bank:
                desc_lines.append(f"{cp_role} Bank: {cp_bank}")
            # Bank BIC
            if cp_bank_bic:
                desc_lines.append(f"{cp_role} Bank BIC: {cp_bank_bic}")

        # Optional technical info
        if oper_type or transaction_code:
            tech_parts = []
            if oper_type:
                tech_parts.append(f"OpType: {oper_type}")
            if transaction_code:
                tech_parts.append(f"TxnCode: {transaction_code}")
            if tech_parts:
                desc_lines.append(" / ".join(tech_parts))

        description = "\n".join(desc_lines)

        transactions.append({
            "date": posting_date,
            "description": description,
            "deposit": deposit,
            "withdrawal": withdrawal,
            "bank_balance": running_balance,
            "reference_number": document_number,
            "currency": currency_code,
            "cp_role": cp_role,
            "cp_name": cp_name,
            "cp_account": cp_account,
            "cp_idno": cp_idno,
            "cp_bank": cp_bank,
            "cp_bank_bic": cp_bank_bic,
        })

    # Prefer dates from header if available, otherwise derived from documents
    # If bank reports closing balance, use it; otherwise trust running balance

    return transactions

def resolve_party_by_idno(tx: dict) -> tuple[str, str]:
    from erpnext_moldova_banking.utils.party import resolve_party_by_idno as _resolve
    return _resolve(tx)

def is_iban_valid(iban_string):
    """
    Checks if a string is a valid IBAN format using a regular expression.
    Returns True if valid, False otherwise.
    """
    # Remove spaces and convert to uppercase for consistent matching
    cleaned_iban = iban_string.replace(" ", "").upper()

    # A general IBAN regex pattern (adjust for more specific country rules if needed)
    iban_pattern = r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$"

    return re.fullmatch(iban_pattern, cleaned_iban)