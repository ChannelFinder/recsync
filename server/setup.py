#!/usr/bin/env python

from distutils.core import setup

setup(
  name="recceiver",
  version="0.1",
  description="resync server",
  author="Michael Davidsaver",
  author_email="mdavidsaver@gmail.com",
  url="https://github.com/mdavidsaver/recsync",
  packages=[
    "recceiver",
    "twisted.plugins",
  ],
  package_data={
    "twisted": ["plugins/recceiver_plugin.py"],
  },
)
