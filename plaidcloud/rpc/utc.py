#!/usr/bin/env python
# coding=utf-8

import calendar
import time
import datetime

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2023, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


def timestamp():
    """Returns Unix Timestamp of curret UTC time"""
    return calendar.timegm(time.gmtime())


def obj():
    """Returns datetime object of current UTC time"""
    return datetime.datetime.utcfromtimestamp(timestamp())


def full():
    """Returns human friendly full date of current UTC time"""
    return obj().strftime("%A, %d %B %Y %I:%M%p")


def iso():
    """Returns ISO 8601 date format of current UTC time"""
    return '{0}+00:00'.format(obj().isoformat())


def iso_timestamp():
    """Returns an ISO 8601 compliant timestamp string"""
    return '{}Z'.format(datetime.datetime.utcnow().replace(microsecond=0).isoformat())


def monthrange(year, month):
    """Returns calendar object for the given month

    Args:
        year (int): The calendar year to get
        month (int): The month to get
    """
    return calendar.monthrange(year, month)
