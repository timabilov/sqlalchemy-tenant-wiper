#!/usr/bin/env python
"""Setup script for sqlalchemy-tenant-wiper package."""

import os

from setuptools import find_packages, setup


# Read the README file
def read_file(filename):
    """Read file contents."""
    here = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(here, filename), 'r', encoding='utf-8') as f:
        return f.read()

# Read version from package
def get_version():
    """Get version from package __init__.py."""
    here = os.path.abspath(os.path.dirname(__file__))
    version_file = os.path.join(here, 'sqlalchemy_tenant_wiper', '__init__.py')
    with open(version_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.startswith('__version__'):
                return line.split('=')[1].strip().strip('"').strip("'")
    raise RuntimeError('Unable to find version string.')

setup(
    name='sqlalchemy-tenant-wiper',
    version=get_version(),
    author='Your Name',
    author_email='your.email@example.com',
    description='A flexible SQLAlchemy-based library for tenant data deletion in multi-tenant applications',
    long_description=read_file('README.md'),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/sqlalchemy-tenant-wiper',
    project_urls={
        'Bug Reports': 'https://github.com/yourusername/sqlalchemy-tenant-wiper/issues',
        'Source': 'https://github.com/yourusername/sqlalchemy-tenant-wiper',
        'Documentation': 'https://github.com/yourusername/sqlalchemy-tenant-wiper#readme',
    },
    packages=find_packages(exclude=['tests', 'tests.*']),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'SQLAlchemy>=1.4.0,<3.0.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
            'ruff>=0.1.0',
            'mypy>=1.0.0',
            'pre-commit>=2.0.0',
        ],
        'test': [
            'pytest>=7.0.0',
            'pytest-cov>=4.0.0',
        ],
    },
    keywords='sqlalchemy tenant multi-tenant database deletion cleanup',
    include_package_data=True,
    zip_safe=False,
)
