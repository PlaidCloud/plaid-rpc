from plaidcloud.rpc import identifiers


def test_the_prefixes_every_existing_schema_and_table_carries():
    """Renaming either would orphan every schema or table already created under it."""
    assert (identifiers.SCHEMA_PREFIX, identifiers.TABLE_PREFIX) == ('anlz', 'analyzetable_')
