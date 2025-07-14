"""SQLAlchemy Tenant Wiper - A flexible library for tenant data deletion in SaaS applications."""

__version__ = '0.1.8'
__author__ = 'Tim'
__email__ = 'tim@skripe.com'

from .core import TenantDeleter, TenantWiperConfig

__all__ = [
    'TenantWiperConfig',
    'TenantDeleter',
]
