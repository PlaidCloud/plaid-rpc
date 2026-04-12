#!/usr/bin/env python
# coding=utf-8

from unittest import mock

from plaidcloud.rpc.ratelimiter import RateLimiter


class TestRateLimiter:

    def test_init_defaults(self):
        rl = RateLimiter()
        assert rl._capacity == 1000
        assert rl._add_rate == 10

    def test_init_custom(self):
        rl = RateLimiter(bucket_capacity=5, token_add_rate=1)
        assert rl._capacity == 5
        assert rl._add_rate == 1

    def test_first_request_not_dropped(self):
        rl = RateLimiter(bucket_capacity=10)
        assert rl.check_to_drop('user1') is False

    def test_bucket_drains(self):
        rl = RateLimiter(bucket_capacity=3, token_add_rate=0)
        assert rl.check_to_drop('user1') is False  # 3 -> 2
        assert rl.check_to_drop('user1') is False  # 2 -> 1
        assert rl.check_to_drop('user1') is False  # 1 -> 0
        assert rl.check_to_drop('user1') is True   # 0, dropped

    def test_separate_keys_have_separate_buckets(self):
        rl = RateLimiter(bucket_capacity=1, token_add_rate=0)
        assert rl.check_to_drop('user1') is False
        assert rl.check_to_drop('user1') is True   # user1 drained
        assert rl.check_to_drop('user2') is False   # user2 still has tokens

    def test_tokens_refill_over_time(self):
        rl = RateLimiter(bucket_capacity=5, token_add_rate=10)
        # Drain the bucket
        for _ in range(5):
            rl.check_to_drop('user1')

        # Now it's empty
        assert rl.check_to_drop('user1') is True

        # Simulate time passing (1 second at rate 10 = 10 tokens added, capped at 5)
        with mock.patch('plaidcloud.rpc.ratelimiter.time.time', return_value=rl._buckets['user1']['time'] + 1.0):
            assert rl.check_to_drop('user1') is False

    def test_refill_capped_at_capacity(self):
        rl = RateLimiter(bucket_capacity=3, token_add_rate=100)
        rl.check_to_drop('user1')  # Use one token

        # Even with lots of time passing, tokens are capped at capacity
        with mock.patch('plaidcloud.rpc.ratelimiter.time.time', return_value=rl._buckets['user1']['time'] + 100.0):
            rl.check_to_drop('user1')
            assert rl._buckets['user1']['count'] <= 3
