#!/usr/bin/env python

from distutils.core import setup

setup(
    name='influxdb_aggregation',
    version='0.1',
    description='A tool for managing InfluxDB graduated series',
    author='Christoffer Viken',
    author_email='christoffer@viken.me',
    url='https://github.com/CVi/influxdb_aggregation',
    packages=['influxdb_aggregation'],
    scripts=['scripts/influx_retention'],
)
