# Changelog

## Unreleased

- Add `is_dialect_databricks_based` and extend the Plaid* type decorators for the Snowflake/Databricks lakehouse backends ([sc-23158](https://app.shortcut.com/plaidcloud/story/23158)): `PlaidNumeric` → Databricks `DECIMAL(38, 10)`; `PlaidTimestamp` → Databricks `TIMESTAMP_NTZ` (Snowflake rides the plain `TIMESTAMP` impl via session `TIMESTAMP_TYPE_MAPPING`); `PlaidUnicode` → Snowflake `VARCHAR`, Databricks `STRING`; `PlaidTinyInt` → Databricks `TINYINT`; `PlaidJSON` → `VARIANT` on both. Vendor dialects stay lazily imported — nothing new is required at module load ([@inviscid](https://github.com/inviscid)).

- Add a `currency` analyze data type: the new `PlaidCurrency` SQLAlchemy type renders an explicit `DECIMAL(18, 4)` on every warehouse dialect (8-byte Decimal64 values on Databend), with matching pandas (`float64`) and arrow (`decimal128(18, 4)`) conversions. Type inference is unchanged — `currency` source-type spellings still map to `numeric`; only an explicitly stored `currency` dtype gets the narrow type ([@inviscid](https://github.com/inviscid)).
