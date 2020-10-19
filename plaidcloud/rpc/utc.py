#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import
import calendar
import time
import datetime


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
