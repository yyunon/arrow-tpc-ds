#!/usr/bin/env python3

import os
from setuptools import setup

setup(name='tpcarrow',
      version='0.1',
      description='This project creates arrow based schemas with fletcher interface for TPC-DS/H datasets.',
      author='Yuksel Yonsel',
      license='Apache License 2.0',
      pymodules=['tpcarrow'],
      packages=['tpcarrow'],
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: Apache License 2.0"
      ],
      install_requires=['pyarrow==1.0'],
      entry_points={'console_scripts': ['tpcarrow=tpcarrow:cli']},
      python_requires='>=3.6',
      include_package_data=True)
