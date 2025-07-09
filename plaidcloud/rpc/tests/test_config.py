#!/usr/bin/env python
# coding=utf-8

import unittest
import pytest

from plaidcloud.rpc.config import PlaidConfig
__author__ = "Pat Buxton"
__copyright__ = "© Copyright 2022, Tartan Solutions, Inc"
__credits__ = ["Pat Buxton"]
__license__ = "Apache 2.0"
__maintainer__ = "Pat Buxton"
__email__ = "patrick.buxton@tartansolutions.com"


class TestConfigWithNoPaths(unittest.TestCase):
    """These tests validate all aspects of the PlaidConfig object"""

    def setUp(self):
        self.config = PlaidConfig(config_path='plaidcloud/rpc/tests/.plaid/plaid.conf')

    def test_paths_is_empty(self):
        assert self.config.paths == {}

    def test_raises_on_reading_specific_path(self):
        with pytest.raises(Exception):
            x = self.config.path('something')

    def tearDown(self):
        pass
