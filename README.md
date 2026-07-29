### ERPNext Moldova Banking

Frappe/ERPNext app for Moldova banking workflows: MAIB API (statements & payments), DBO file import, bank transaction automation, BNM FX rates, and Telegram notifications.

Requires **ERPNext v15** / Frappe v15.

### Features

#### Moldova Banking Settings
Single DocType that groups all configuration:

| Tab | Purpose |
|---|---|
| **IDNO** | Map Company / Customer / Supplier tax-id fields for party matching |
| **Automation** | Rules that create Payment Entry / Journal Entry from submitted Bank Transactions |
| **MAIB API** | OAuth credentials, statement sync, outward payments, auto Payment Entry from statement |
| **Telegram** | Bot notifications for new Bank Transactions (filters + message fields) |
| **Exchange Rates** | BNM rates API key and Currency Exchange Settings helper |

#### MAIB statement sync
- Manual **Fetch Statement** and scheduled sync per bank account
- Parses account-statement XML into `Bank Transaction` (dedupe via `unique_key`)
- Optionally loads Transfer Details when MAIB accepts the identity; otherwise keeps the statement description
- Stepped progress UI for manual fetch (list → per-transaction details/create)

#### MAIB outward payments
- Send Ordinary MDL transfers from **Payment Order**
- Status polling (`Waiting` / `In Process`)
- Optional **Auto Payment Entry from Statement**: match executed PO ↔ BT → PE → reconcile

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

### Installation

```bash
cd $PATH_TO_YOUR_BENCH
bench get-app https://github.com/evghenin/erpnext_moldova_banking.git --branch master
bench --site <site> install-app erpnext_moldova_banking
bench --site <site> migrate
```

Open **Moldova Banking Settings** and configure the tabs you need (MAIB Test/Production, Telegram bot token & chat id, etc.).

### Tests

```bash
bench --site <site> run-tests --app erpnext_moldova_banking
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
