# coding=utf-8

from __future__ import absolute_import
import unittest
from nose.tools import assert_equal, assert_raises, assert_true, assert_false

from plaidcloud.rpc.remote.auth import Auth

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2017, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache 2.0"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"


class TestAuth(unittest.TestCase):

    """These tests validate the Remote Authentication object and methods"""

    def setUp(self):
        "Constructs a test environment if necessary"
        pass

    def test_object_instantiation(self):
        obj = Auth()

    def test_is_ok(self):
        obj = Auth()
        obj._auth_status = u'ok'
        assert_true(obj.is_ok())

        conditions_not_ok = (u'fail', u'ready', u'setup')
        for cno in conditions_not_ok:
            obj._auth_status = cno
            assert_false(obj.is_ok())

    def test_user_based_auth(self):
        obj = Auth()
        user_name = 'test_public'
        password = 'test_private'
        obj.user(user_name, password)

        assert_equal(obj._public_key, user_name)
        assert_equal(obj._private_key, password)
        assert_equal(obj._auth_method, u'user')

    def test_user_based_mfa_auth(self):
        obj = Auth()
        user_name = 'test_public'
        password = 'test_private'
        mfa = '12345678ABC!$*'
        obj.user(user_name, password, mfa)

        assert_equal(obj._public_key, user_name)
        assert_equal(obj._private_key, password)
        assert_equal(obj._mfa, mfa)
        assert_equal(obj._auth_method, u'user')

    def test_agent_based_auth(self):
        obj = Auth()
        public_key = 'test_public'
        private_key = 'test_private'
        obj.agent(public_key, private_key)

        assert_equal(obj._public_key, public_key)
        assert_equal(obj._private_key, private_key)
        assert_equal(obj._auth_method, u'agent')

    def test_transform_based_auth(self):
        obj = Auth()
        task_id = 'test_public'
        session_id = 'test_private'
        auth_method = u'transform'
        obj.agent(task_id, session_id, auth_method)

        assert_equal(obj._public_key, task_id)
        assert_equal(obj._private_key, session_id)
        assert_equal(obj._auth_method, u'transform')

    def test_get_method(self):
        obj = Auth()
        obj.set_method(u'user')

        assert_equal(obj.get_method(), u'user')

    def test_get_status(self):
        obj = Auth()
        obj._auth_status = u'setup'

        assert_equal(obj.get_auth_status(), u'setup')

    def test_get_status_message(self):
        obj = Auth()
        message = u'yo ho ho and a bottle of rum!'
        obj._status_message = message

        assert_equal(obj.get_status_message(), message)

    def test_get_attempts(self):
        obj = Auth()
        obj._attempts = 555

        assert_equal(obj.get_attempts(), 555)

    def test_get_public_key(self):
        obj = Auth()
        obj._public_key = u'test_public_key'

        assert_equal(obj._get_public_key(), u'test_public_key')

    def test_get_private_key(self):
        obj = Auth()
        obj._private_key = u'test_private_key'

        assert_equal(obj._get_private_key(), u'test_private_key')

    def test_get_mfa(self):
        obj = Auth()
        obj._mfa = u'mfa value 123'

        assert_equal(obj._get_mfa(), u'mfa value 123')

    def test_set_method(self):
        obj = Auth()

        legit_methods = (u'user', u'agent', u'transform')

        for lm in legit_methods:
            obj.set_method(lm)

        not_legit_methods = ('blah', 123, {'user': None}, list('user'), None)

        for nlm in not_legit_methods:
            assert_raises(Exception, obj.set_status, nlm)

    def test_set_status_positive(self):
        obj = Auth()

        legit_status = (u'setup', u'ready', u'ok', u'fail')

        for ls in legit_status:
            obj.set_status(ls)

        not_legit_status = ('blah', 123, {'ok': None}, list('ok'), None)

        for nls in not_legit_status:
            assert_raises(Exception, obj.set_status, nls)

    def test_get_package(self):
        obj = Auth()
        user_name = 'test_public'
        password = 'test_private'
        mfa = '12345678ABC!$*'
        obj.user(user_name, password, mfa)

        package = obj.get_package()

        assert_equal(package['PlaidCloud-Auth-Method'], str(u'user'))
        assert_equal(package['PlaidCloud-Key'], user_name)
        assert_equal(package['PlaidCloud-Pass'], password)
        assert_equal(package['PlaidCloud-MFA'], mfa)

    def tearDown(self):
        "Clean up any test structure or records generated during the testing"
        pass

if __name__ == '__main__':
    unittest.TestProgram()
