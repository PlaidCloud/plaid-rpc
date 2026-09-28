#!/usr/bin/env python
"""How PlaidCloud names what it keeps in a warehouse.

A project's schema and each of its tables carry these prefixes, which is how a
reference already naming a table id is told apart from a table's path or name.
plaidcloud-utilities' `query` re-exports both.
"""

__author__ = 'PlaidCloud'
__copyright__ = '© Copyright 2026, PlaidCloud, Inc'
__license__ = 'Apache 2.0'

SCHEMA_PREFIX = 'anlz'
TABLE_PREFIX = 'analyzetable_'
