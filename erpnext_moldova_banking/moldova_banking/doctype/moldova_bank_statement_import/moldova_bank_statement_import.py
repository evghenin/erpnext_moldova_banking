import os
import frappe
from frappe.model.document import Document
from frappe.utils import getdate, flt


class MoldovaBankStatementImport(Document):
    def validate(self):
        """Validate presence of attached file before submit."""
        if not self.file:
            frappe.throw("Please attach bank statement file (.dbo / .txt).")

    def on_submit(self):
        """Main import workflow executed on submit."""
        try:
            header, document_blocks = self._parse_statement_file()
            transactions = self._build_transactions(header, document_blocks)
            created_count, duplicate_count, duplicate_messages = self._create_bank_transactions(transactions)
            self._update_header_fields(header, transactions)

            # Status with duplicates info
            if duplicate_count:
                self.status = f"Imported {created_count} transactions, skipped {duplicate_count} duplicates"
            else:
                self.status = f"Imported {created_count} transactions"

            # Build log
            log_lines = []
            if duplicate_count:
                log_lines.append("Duplicates skipped during import:")
                log_lines.extend(duplicate_messages)
            else:
                log_lines.append("Import completed successfully.")

            self.log = "\n".join(log_lines)

        except Exception as e:
            frappe.log_error(frappe.get_traceback(), "Moldova Bank Statement Import Error")
            self.status = "Error"
            self.log = str(e)
            frappe.throw(f"Error during import: {e}")

    # ---------------------------------------------------------------------
    # File helpers
    # ---------------------------------------------------------------------

    def _get_local_file_path(self) -> str:
        """Resolve Attach field path (checks private/public files)."""
        file_url = self.file
        if not file_url:
            frappe.throw("Bank statement file is not attached.")

        filename = os.path.basename(file_url)

        private_path = frappe.get_site_path("private", "files", filename)
        public_path = frappe.get_site_path("public", "files", filename)

        if os.path.exists(private_path):
            return private_path
        if os.path.exists(public_path):
            return public_path

        frappe.throw(f"File not found on server: {filename}")

    def _parse_statement_file(self):
        """
        Parse text statement file in key=value format with sections:
        - global header (BEGINDATE, ENDDATE, etc.)
        - SECTIONACCOUNTSTART ... SECTIONACCOUNTSTOP (account summary)
        - repeated DocStart ... DocEnd blocks (documents).

        Returns:
            (header_dict, list_of_document_dicts)
        """
        path = self._get_local_file_path()

        # Adjust encoding depending on source bank export
        with open(path, "r", encoding="utf-8") as f:
            raw = f.read()

        lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]

        header = {}
        documents = []
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
                    documents.append(current_doc)
                    current_doc = None
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip().upper()
                value = value.strip()

                if in_account_section:
                    # Account-level fields (ACCOUNT, STARTREST, STOPREST, CURRCODE, etc.)
                    header[key] = value

                elif current_doc is not None:
                    # Document-level fields (DOCUMENTNUMBER, AMOUNT, GROUND, etc.)
                    current_doc[key] = value

                else:
                    # Global header fields (BEGINDATE, ENDDATE)
                    header[key] = value

        return header, documents

    def _parse_date(self, value: str):
        """Parse date in DD.MM.YYYY format into python date."""
        value = (value or "").strip()
        if not value:
            return None
        return getdate(value)

    # ---------------------------------------------------------------------
    # Business logic
    # ---------------------------------------------------------------------

    def _build_transactions(self, header: dict, docs: list[dict]) -> list[dict]:
        """
        Convert parsed document blocks into a list of transaction dicts ready
        to be converted into Bank Transaction documents.

        Expected transaction dict keys:
        - date
        - description
        - deposit
        - withdrawal
        - bank_balance
        - reference_number
        - currency
        - cp_role              (Payer / Receiver)
        - cp_name
        - cp_account
        - cp_idno
        - cp_bank
        - cp_bank_bic
        """
        account_iban = header.get("ACCOUNT")
        opening_balance = flt(header.get("STARTREST") or 0)
        closing_balance_bank = flt(header.get("STOPREST") or 0)
        begin_date_str = header.get("BEGINDATE")
        end_date_str = header.get("ENDDATE")
        currency_code = (header.get("CURRCODE") or "").strip() or None

        transactions: list[dict] = []
        running_balance = opening_balance
        from_date = None
        to_date = None

        for doc in docs:
            # Raw fields from statement
            document_number = (doc.get("DOCUMENTNUMBER") or "").strip()
            document_date_str = (doc.get("DOCUMENTDATE") or "").strip()
            date_written_str = (doc.get("DATEWRITTEN") or "").strip()

            posting_date = self._parse_date(document_date_str or date_written_str)

            amount = flt(doc.get("AMOUNT") or 0)

            payer_account = (doc.get("PAYERACCOUNT") or "").strip()
            receiver_account = (doc.get("RECEIVERACCOUNT") or "").strip()

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
            desc_lines = []

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
        self.statement_from_date = self._parse_date(begin_date_str) or from_date
        self.statement_to_date = self._parse_date(end_date_str) or to_date

        self.opening_balance = opening_balance
        # If bank reports closing balance, use it; otherwise trust running balance
        self.closing_balance = closing_balance_bank or running_balance

        return transactions

    def _update_header_fields(self, header: dict, transactions: list[dict]):
        """
        Placeholder for any extra header-level logic.
        For example: comparing closing balance vs reported STOPREST.
        """
        pass

    # ---------------------------------------------------------------------
    # Creation of ERPNext Bank Transaction docs + duplicate check + party
    # ---------------------------------------------------------------------

    def _create_bank_transactions(self, transactions: list[dict]):
        """
        Create Bank Transaction documents based on prepared transaction dicts.

        Duplicate detection rules:
        - company = this document's company
        - bank_account = this document's bank_account
        - date = tx["date"]
        - docstatus in (0, 1)  # Draft and Submitted
        - and a combination of:
            - deposit / withdrawal
            - reference_number (if present)
            - if reference_number is empty, description is also used

        Canceled transactions (docstatus = 2) are ignored (not treated as duplicates).
        """
        created_count = 0
        duplicate_count = 0
        duplicate_messages: list[str] = []

        for tx in transactions:
            if not tx["date"]:
                # Skip transactions without date
                continue

            # Build duplicate search filters
            filters = {
                "company": self.company,
                "bank_account": self.bank_account,
                "date": tx["date"],
                "docstatus": ["in", [0, 1]],  # Draft and Submitted only
            }

            # Include amount fields in filters if they are non-zero
            if tx["deposit"]:
                filters["deposit"] = tx["deposit"]
            if tx["withdrawal"]:
                filters["withdrawal"] = tx["withdrawal"]

            # If reference_number exists, use it for matching
            if tx["reference_number"]:
                filters["reference_number"] = tx["reference_number"]
            else:
                # If there is no reference number, use description as an additional key
                if tx["description"]:
                    filters["description"] = tx["description"]

            # Check for existing transactions
            existing = frappe.get_all(
                "Bank Transaction",
                filters=filters,
                pluck="name",
                limit=1,
            )

            if existing:
                # Duplicate found - log and skip creation
                duplicate_count += 1
                existing_name = existing[0]
                msg = (
                    f"- Duplicate: Bank Transaction {existing_name}, "
                    f"date={tx['date']}, deposit={tx['deposit']}, "
                    f"withdrawal={tx['withdrawal']}, ref={tx['reference_number'] or ''}"
                )
                duplicate_messages.append(msg)
                continue

            # No duplicate found -> create new Bank Transaction
            bt = frappe.new_doc("Bank Transaction")
            bt.company = self.company
            bt.bank_account = self.bank_account

            # Core fields used by Bank Reconciliation Tool
            bt.date = tx["date"]
            bt.deposit = tx["deposit"]
            bt.withdrawal = tx["withdrawal"]
            bt.description = tx["description"]

            # Reference number
            if hasattr(bt, "reference_number"):
                bt.reference_number = tx["reference_number"]

            # Bank party details (counterparty from statement)
            self._set_bank_party_fields(bt, tx)

            # Currency: prefer statement currency already set on the import doc
            if hasattr(bt, "currency"):
                if getattr(self, "statement_currency", None):
                    bt.currency = self.statement_currency
                elif tx["currency"]:
                    bt.currency = tx["currency"]
                else:
                    bt.currency = frappe.db.get_value(
                        "Bank Account",
                        self.bank_account,
                        "account_currency",
                    )

            # Optional: bank balance if field exists
            if hasattr(bt, "bank_balance"):
                bt.bank_balance = tx["bank_balance"]

            # Automatic party matching by IDNO (Customer / Supplier)
            self._assign_party_by_idno(bt, tx)

            bt.insert()
            created_count += 1

        return created_count, duplicate_count, duplicate_messages

    # ---------------------------------------------------------------------
    # Helpers for bank party and party matching
    # ---------------------------------------------------------------------

    def _set_bank_party_fields(self, bt, tx: dict):
        """
        Fill bank_party_name, bank_party_iban / bank_party_account_number
        based on counterparty info from the statement.
        """
        cp_name = (tx.get("cp_name") or "").strip()
        cp_account = (tx.get("cp_account") or "").strip()

        if hasattr(bt, "bank_party_name") and cp_name:
            bt.bank_party_name = cp_name

        if not cp_account:
            return

        # Simple IBAN detection (Moldovan IBAN starts with 'MD', usually length >= 24)
        account_normalized = cp_account.replace(" ", "")
        is_iban = account_normalized.upper().startswith("MD") or len(account_normalized) >= 20

        if is_iban and hasattr(bt, "bank_party_iban"):
            bt.bank_party_iban = cp_account
        elif hasattr(bt, "bank_party_account_number"):
            bt.bank_party_account_number = cp_account

    def _assign_party_by_idno(self, bt, tx: dict):
        """
        Automatically set party_type and party based on IDNO from bank statement
        and configuration in 'Moldova Banking Settings'.

        Rules:
        - For incoming payments (deposit > 0, withdrawal == 0) -> search Customer.
        - For outgoing payments (withdrawal > 0, deposit == 0) -> search Supplier.
        - IDNO taken from counterparty field in statement (cp_idno).
        - IDNO field names for Customer/Supplier are defined in Moldova Banking Settings.
        """
        cp_idno = (tx.get("cp_idno") or "").strip()
        if not cp_idno:
            return

        try:
            settings = frappe.get_single("Moldova Banking Settings")
        except Exception:
            # If settings do not exist or cannot be loaded, skip matching silently
            return

        customer_idno_field = (getattr(settings, "customer_idno_field", None) or "").strip()
        supplier_idno_field = (getattr(settings, "supplier_idno_field", None) or "").strip()

        # Incoming payment -> Customer
        if bt.deposit and not bt.withdrawal and customer_idno_field:
            customer = frappe.get_all(
                "Customer",
                filters={customer_idno_field: cp_idno},
                pluck="name",
                limit=1,
            )
            if customer:
                bt.party_type = "Customer"
                bt.party = customer[0]
                return

        # Outgoing payment -> Supplier
        if bt.withdrawal and not bt.deposit and supplier_idno_field:
            supplier = frappe.get_all(
                "Supplier",
                filters={supplier_idno_field: cp_idno},
                pluck="name",
                limit=1,
            )
            if supplier:
                bt.party_type = "Supplier"
                bt.party = supplier[0]
                return
