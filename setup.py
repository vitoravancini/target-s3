#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-s3",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_s3"],
    install_requires=[
        "singer-python>=5.0.12",
        "boto3"
    ],
    entry_points="""
    [console_scripts]
    target-s3=target_s3:main
    """,
    packages=["target_s3"],
    package_data = {},
    include_package_data=True,
)
