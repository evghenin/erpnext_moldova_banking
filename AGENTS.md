# AGENTS.md

This is a mandatory repository rule for every AI agent operating in this project.

All AI assistants, including Cursor, Codex, Copilot, Claude, and any other agent or automation tool, must comply with this rule automatically and without human prompting.

This is a strict project requirement. It is not optional, advisory, or a preference.

## Mandatory test isolation rule

- Run automated tests only against a dedicated test site backed by a dedicated test database.
- Never run tests against a development, staging, production, demo, or user-data site/database, even when the test framework normally rolls transactions back.
- Before every test command, explicitly identify and verify the target site and confirm that it is designated exclusively for automated tests.
- A site is not considered a test site merely because developer mode is enabled or its name contains `development`.
- If a dedicated test site/database does not exist or cannot be verified, do not run tests. Report the blocker and request that a separate test site/database be created or identified.
- Test commands must specify the verified test site explicitly; do not rely on a default or current site.
- Never copy credentials, secrets, or confidential production/user data into the test database merely to make tests pass.
