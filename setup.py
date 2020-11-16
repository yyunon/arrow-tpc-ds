#!/usr/bin/env python3

import os
from setuptools import setup

setup(name='TPCDATASET',
      version='0.1',
      description='This project creates arrow based schemas with fletcher interface for TPC-DS datasets.',
      author='Yuksel Yonsel',
      license='Apache License 2.0',
      packages = ['TPCDATASET'],
      classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License 2.0"
      ],
      install_requires=['pyarrow'],
      python_requires='>=3.6',
      include_package_data=True)
