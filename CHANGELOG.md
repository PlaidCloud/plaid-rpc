# Changelog

## Unreleased

- Add a `currency` analyze data type: the new `PlaidCurrency` SQLAlchemy type renders an explicit `DECIMAL(18, 4)` on every warehouse dialect (8-byte Decimal64 values on Databend), with matching pandas (`float64`) and arrow (`decimal128(18, 4)`) conversions. Type inference is unchanged — `currency` source-type spellings still map to `numeric`; only an explicitly stored `currency` dtype gets the narrow type ([@inviscid](https://github.com/inviscid)).
