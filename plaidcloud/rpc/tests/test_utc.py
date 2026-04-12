#!/usr/bin/env python
# coding=utf-8

import datetime
import re

from plaidcloud.rpc import utc


class TestTimestamp:

    def test_returns_integer(self):
        result = utc.timestamp()
        assert isinstance(result, int)

    def test_is_reasonable_epoch(self):
        result = utc.timestamp()
        # Should be after 2020 and before 2100
        assert result > 1577836800
        assert result < 4102444800


class TestObj:

    def test_returns_datetime(self):
        result = utc.obj()
        assert isinstance(result, datetime.datetime)

    def test_has_utc_timezone(self):
        result = utc.obj()
        assert result.tzinfo == datetime.UTC


class TestFull:

    def test_returns_string(self):
        result = utc.full()
        assert isinstance(result, str)

    def test_contains_year(self):
        result = utc.full()
        year = str(datetime.datetime.now(datetime.UTC).year)
        assert year in result


class TestIso:

    def test_returns_iso_format(self):
        result = utc.iso()
        assert isinstance(result, str)
        assert 'T' in result


class TestIsoTimestamp:

    def test_returns_iso_format(self):
        result = utc.iso_timestamp()
        assert isinstance(result, str)
        assert re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', result)

    def test_no_microseconds(self):
        result = utc.iso_timestamp()
        assert '.' not in result


class TestMonthrange:

    def test_january(self):
        weekday, days = utc.monthrange(2024, 1)
        assert days == 31

    def test_february_leap_year(self):
        weekday, days = utc.monthrange(2024, 2)
        assert days == 29

    def test_february_non_leap_year(self):
        weekday, days = utc.monthrange(2023, 2)
        assert days == 28

    def test_april(self):
        weekday, days = utc.monthrange(2024, 4)
        assert days == 30
