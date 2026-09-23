### ERPNext Moldova Banking

Frappe/ERPNext app for Moldova banking workflows: MAIB API (statements and payments), DBO file import, bank transaction automation, BNM FX rates, and Telegram notifications.

**Current version: 2.1.0** (ERPNext v15 / Frappe v15).

| Line | What it is |
|---|---|
| Tag [`v1.0.0`](https://github.com/evghenin/erpnext_moldova_banking/releases/tag/v1.0.0) | Last release before MAIB API (file import, automation, BNM) |
| Tag [`v2.0.0`](https://github.com/evghenin/erpnext_moldova_banking/releases/tag/v2.0.0) | MAIB API, Bank Payment Instruction for suppliers, Active Hours, Telegram |
| Tag [`v2.1.0`](https://github.com/evghenin/erpnext_moldova_banking/releases/tag/v2.1.0) and branches `master` and `v2` | Shareholder and Employee payments, company bank account from the party default |

### Features

#### Moldova Banking Settings
Single DocType that groups configuration:

| Tab | Purpose |
|---|---|
| **Details** | **Enable Active Hours** plus weekday/time windows; map Company, Customer, Supplier, Shareholder, and Employee tax-id fields; Romanian language for payment descriptions |
| **Automation** | Rules that create Payment Entry / Journal Entry from submitted Bank Transactions |
| **MAIB API** | Per-company OAuth credentials, Test/Production URLs, statement sync, outward payments, auto Payment Entry from statement |
| **Telegram** | Bot notifications for new Bank Transactions (filters + message fields) |
| **Exchange Rates** | BNM rates API key and Currency Exchange Settings helper |

When **Enable Active Hours** is on, at least one period is required (day of week, time from, time to). Same-day windows must have Time From earlier than Time To. Overnight windows are allowed by setting Time From later than Time To (for example Tuesday 22:00–06:00). Scheduled MAIB API statement sync runs only inside a matching period. Manual **Fetch Statement** is not restricted. Statement Sync Accounts still control frequency (every 15 minutes, etc.) per bank account.

Residency (`Resident` / `Non-Resident`) is stored on Company, Customer, and Supplier. The field is created on migrate/install if another Moldova app has not already added it.

**Tax ID** (`tax_id`, unique) is added on Shareholder (after Title) and Employee (after Bank Name) on migrate/install when that field is not already present. Shareholder IDNO field and Employee IDNO field in settings default to `tax_id`.

#### MAIB statement sync
- Manual **Fetch Statement** and scheduled sync per bank account
- Scheduled API sync is skipped outside **Active Hours** when that feature is enabled and periods are configured
- Parses account-statement XML into `Bank Transaction` (dedupe via `unique_key`)
- Optionally loads Transfer Details when MAIB accepts the identity; otherwise keeps the statement description
- Stepped progress UI for manual fetch (list → per-transaction details/create)

#### MAIB outward payments
- **Bank Payment Instruction** (not Payment Order): payment date, payer IBAN, beneficiary, residency, amount, and a required instruction to the bank
- Party types: Company, Customer, Supplier, Shareholder, Employee
- **Company Bank Account** comes from the Customer or Supplier **Default Bank Account** when that account belongs to the paying company; otherwise the company bank account marked default
- Supplier and Customer payments list one or more **Purchase Invoices** (also created from a Purchase Invoice). Amount and the instruction to the bank are filled from those invoices
- Company, Shareholder, and Employee payments hide the invoice table. Amount and the instruction to the bank are entered manually
- Send Ordinary MDL transfers, poll status
- Optional **Auto Payment Entry from Statement**: match executed instruction ↔ Bank Transaction → Payment Entry → reconcile

#### DBO / file import
- **Moldova Bank Statement Import** for MAIB DBO statement files (current/card accounts)
- Same unique-key dedupe and party resolution by IDNO

#### Telegram notifications
One message per new Bank Transaction when enabled.

Filters:
- via file import / via API
- incoming / outgoing
- matched Automation / not matched Automation

Message fields (optional): company, sender/receiver name, IDNO/IDNP, amount+currency, description, payment date.

Header format: `Incoming Bank Transaction ACC-BTN-…` / `Outgoing Bank Transaction ACC-BTN-…`.

#### BNM exchange rates
Protected endpoint + helpers to regenerate the API key and wire ERPNext **Currency Exchange Settings**.

### Installation (v2.1.0)

```bash
cd $PATH_TO_YOUR_BENCH
bench get-app https://github.com/evghenin/erpnext_moldova_banking.git --branch v2
bench --site <site> install-app erpnext_moldova_banking
bench --site <site> migrate
```

`--branch master` currently tracks the same v2.1.0 line.

Open **Moldova Banking Settings** and configure the tabs you need (MAIB company credentials, Test/Production, Telegram bot token & chat id, etc.).

### Upgrade from v2.0.0

1. Update the app to tag `v2.1.0` (or `master` / `v2`).
2. Run `bench --site <site> migrate`.

Migrate adds Shareholder and Employee IDNO settings, creates **Tax ID** on those doctypes when missing, and extends Bank Payment Instruction with those party types.

### Upgrade from v1.0.0

1. Update the app to `v2` (or `master`).
2. Run `bench --site <site> migrate`.

Migrate adds MAIB/BPI schema and runs a single patch that removes v1 **POS Clearing Rule** and the orphan **ERPNext Moldova Banking** module. There is no Payment Order field migration: v1 never had MAIB custom fields on Payment Order.

### Tests

Use a **dedicated test site and database** only. Do not run tests against development, staging, production, or any site that holds user data.

```bash
bench --site <dedicated-test-site> run-tests --app erpnext_moldova_banking
```

### Contributing

This app uses `pre-commit` for code formatting and linting. Please [install pre-commit](https://pre-commit.com/#installation) and enable it for this repository:

```bash
cd apps/erpnext_moldova_banking
pre-commit install
```

Pre-commit is configured to use:

- ruff
- eslint
- prettier
- pyupgrade

### CI

GitHub Actions workflows (when configured):

- **CI**: installs the app and runs unit tests
- **Linters**: Frappe Semgrep Rules and pip-audit on pull requests

### License

mit
