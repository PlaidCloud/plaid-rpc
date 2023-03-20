#!/usr/bin/env python
# coding=utf-8
"""Contains RateLimiter."""

import time

__author__ = "Brian McFadden"
__copyright__ = "Copyright 2011, Tartan Solutions, Inc."
__credits__ = ["Paul Morel", "Brian McFadden", "Michael Rea", "Dan Hummon"]
__license__ = "Apache 2.0"
__maintainer__ = "Tartan Solutions"
__email__ = "brian.mcfadden@tartansolutions.com"

class RateLimiter():
    """Performs rate limiting based on the token bucket algorithm."""
    
    def __init__(self, bucket_capacity=1000, token_add_rate=10):
        """Creates a RateLimiter, which saves states in a dict.
        Each bucket starts off with a capacity of size bucket_capacity and
        has one token added for every token_add_rate seconds passed up until
        bucket_capacity."""
        
        self._buckets = {}
        self._capacity = bucket_capacity
        self._add_rate = token_add_rate
    
    def check_to_drop(self, key):
        """Determines if the connection should be dropped based on a key.
        The key needs to be something that uniquely identifies entity being
        limited, like a session ID, IP address, or username. The algorithm used
        is known as the token bucket algorithm. This method consumes one token
        per use and calculates how many tokens need to be added since the last
        time it was called for a particular key (so no concurrency is involved
        in maintaining the buckets)."""
        
        # dict.get is used in case we haven't seen this key before.
        state = self._buckets.get(key, {})
        curr_time = time.time()
        last_time = state.get('time', curr_time)
        count = state.get('count', self._capacity)
        # For every second that has passed since the last time, add a number
        # of tokens to the bucket equal to defined rate (up until capacity).
        count += int((curr_time - last_time) * self._add_rate)
        count = min(count, self._capacity)
        drop = count < 1
        if not drop:
            # Consume a token if we're letting the request go through.
            count -= 1
        # Save the information.
        state['time'] = curr_time
        state['count'] = count
        self._buckets[key] = state
        
        return drop
