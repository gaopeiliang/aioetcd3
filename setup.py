#!/usr/bin/env python

from setuptools import setup, find_packages

version = "1.11"

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError):
    long_description = ""


setup(
    name="aioetcd3",
    version=version,
    author="gaopeiliang",
    author_email="964911957@qq.com",
    long_description=long_description,
    description="asyncio wrapper for etcd v3",
    license="Apache",
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Framework :: AsyncIO',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development',
        'Framework :: AsyncIO',
    ],
    url="https://github.com/gaopeiliang/aioetcd3",
    platforms=['any'],
    packages=find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'aiogrpc>=1.4',
        'protobuf'
    ]
)
