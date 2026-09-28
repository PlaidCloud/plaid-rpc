#!/usr/bin/env python

from unittest import mock

import pytest

from plaidcloud.rpc.stringtransforms import apply_variables, replaceTags


class TestApplyVariables:

    def test_substitutes_each_variable(self):
        assert apply_variables('{a}-{b}', {'a': 'x', 'b': 2}) == 'x-2'

    def test_a_format_spec_applies_to_the_value(self):
        assert apply_variables('{n:03d}', {'n': 7}) == '007'

    def test_no_message_is_none(self):
        assert apply_variables(None, {'a': 'x'}) is None

    def test_positional_tokens_are_dropped(self):
        assert apply_variables('a{}b', {}) == 'ab'

    def test_a_message_of_only_positional_tokens_is_empty(self):
        assert apply_variables('{}', {}) == ''

    def test_no_variables_leaves_plain_text(self):
        assert apply_variables('plain') == 'plain'

    def test_a_missing_variable_is_refused_by_name(self):
        with pytest.raises(Exception, match=r'invalid or undefined: a, b\.'):
            apply_variables('{b}{a}', {})

    def test_a_missing_variable_is_removed_when_not_strict(self):
        assert apply_variables('x{a}y{b}', {'b': 'B'}, strict=False) == 'xyB'

    def test_the_handler_hears_what_was_missing(self):
        handler = mock.Mock()

        apply_variables('{a}', {}, strict=False, nonstrict_error_handler=handler)

        handler.assert_called_once_with('The following variables are invalid or undefined: a.')

    def test_an_unparseable_message_says_which(self):
        with pytest.raises(Exception, match='Error trying to apply variables to string {a'):
            apply_variables('{a', {'a': 'x'})


class TestReplaceTags:

    def test_a_known_tag_is_replaced(self):
        assert replaceTags('[a] and [b]', {'a': 'A', 'b': 'B'}) == 'A and B'

    def test_an_unknown_tag_is_left(self):
        assert replaceTags('[a] and [c]', {'a': 'A'}) == 'A and [c]'
