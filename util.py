#!/usr/bin/python3 -O
#
# Copyright 2018 Daniel Balparda (balparda@gmail.com)
#
"""Balparda's Misc Utils."""

import logging
# import pdb

__author__ = 'balparda@gmail.com (Daniel Balparda)'
__version__ = (1, 0)


# log format string
LOG_FORMAT = '%(asctime)-15s: %(module)s/%(funcName)s/%(lineno)d: %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)  # set this as default


class Error(Exception):
  """Misc Base exception."""
