import os
import re
import sys
import codecs
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))
src_dir = os.path.join(here, "src")
sys.path.insert(0, src_dir)


def read(*parts):
    return codecs.open(os.path.join(here, *parts), 'r').read()


def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(
        r"^__version__ = ['\"]([^'\"]*)['\"]",
        version_file, re.M,
    )
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


requires = [
    'boto3>=1.4.7'
]


setup_options = dict(
    name='lynk',
    version=find_version("src", "lynk", "__init__.py"),
    description='Client for using AWS DynamoDB as a distributed lock.',
    long_description=open('README.rst').read(),
    author='John Carlyle',
    url='https://github.com/stealthycoin/lynk',
    install_requires=requires,
    scripts=['bin/lynk'],
    package_dir={"": "src"},
    packages=find_packages(where="src", exclude=['tests*']),
    license="Apache License 2.0",
    classifiers=(
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ),
)


setup(**setup_options)
