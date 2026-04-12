#!/usr/bin/env python
# coding=utf-8

import os
import errno
import tempfile

import pytest

from plaidcloud.rpc.file_helpers import makedirs


class TestMakedirs:

    def test_creates_directory(self, tmp_path):
        new_dir = str(tmp_path / 'newdir')
        makedirs(new_dir)
        assert os.path.isdir(new_dir)

    def test_creates_nested_directories(self, tmp_path):
        nested = str(tmp_path / 'a' / 'b' / 'c')
        makedirs(nested)
        assert os.path.isdir(nested)

    def test_existing_directory_no_error(self, tmp_path):
        existing = str(tmp_path / 'existing')
        os.makedirs(existing)
        # Should not raise
        makedirs(existing)
        assert os.path.isdir(existing)

    def test_permission_error_propagates(self, tmp_path):
        # Create a directory, then try to create a sub-directory in a
        # non-existent path that would cause an OSError other than EEXIST
        bad_path = '/nonexistent_root_dir_abc123/test'
        with pytest.raises(OSError):
            makedirs(bad_path)
