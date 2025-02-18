# coding=utf-8

import unittest
import pytest
import datetime
import decimal
import orjson as json

from plaidcloud.rpc.orjson import unsupported_object_json_encoder as enc

__author__ = "Pat Buxton"
__copyright__ = "© Copyright 2020, Tartan Solutions, Inc"
__credits__ = ["Pat Buxton"]
__license__ = "Apache 2.0"
__maintainer__ = "Pat Buxton"
__email__ = "patrick.buxton@tartansolutions.com"


class TestDefaultEncoder(unittest.TestCase):
    """These tests validate all aspects of the default encoder method for orjson"""

    def setUp(self):
        pass

    def test_handles_bytes(self):
        self.assertEqual(json.dumps({'check': bytes('This is some bytes', 'utf-8')}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":"This is some bytes"}')

    def test_handles_decimal(self):
        self.assertEqual(json.dumps({'check': decimal.Decimal('7.345400')}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":"7.345400"}')

    def test_handles_decimal_zero(self):
        self.assertEqual(json.dumps({'check': decimal.Decimal(0.0000000000)}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":"0"}')

    def test_handles_decimal_string_made_zero(self):
        self.assertEqual(json.dumps({'check': decimal.Decimal("0.0000000000")}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":"0"}')

    def test_handles_negative(self):
        self.assertEqual(json.dumps({'check': decimal.Decimal("-390")}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":"-390"}')

    def test_handles_timedelta(self):
        self.assertEqual(json.dumps({'check': datetime.timedelta(days=10, weeks=1, hours=2, minutes=3, microseconds=11)}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":"17 days, 2:03:00.000011"}')

    def test_handles_set(self):
        self.assertEqual(json.dumps({'check': {1, 2, 3}}, default=enc, option=json.OPT_NON_STR_KEYS), b'{"check":[1,2,3]}')

    def test_raises_on_unknown(self):
        class UnsupportedType():
            pass
        with pytest.raises(json.JSONEncodeError):
            json.dumps({'check': UnsupportedType()}, default=enc, option=json.OPT_NON_STR_KEYS)

    def tearDown(self):
        pass
