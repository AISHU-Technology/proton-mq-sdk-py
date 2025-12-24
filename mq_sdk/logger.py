#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""Logging output module"""

import logging


def get_logger(name):
  """Get logger for each module"""
  logger = logging.getLogger(name)
  logger.setLevel(level=logging.DEBUG)

  # If logger already has handlers, don't add duplicates
  if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s: [%(levelname)s] [%(name)s] %(message)s")

    # Only keep console output
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logger.addHandler(console)

  return logger
