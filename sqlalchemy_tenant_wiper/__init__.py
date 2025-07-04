"""SQLAlchemy Tenant Wiper - A flexible library for tenant data deletion in SaaS applications."""

__version__ = '0.1.0'
__author__ = 'Your Name'
__email__ = 'your.email@example.com'

from .core import TenantDeleter, TenantWiperConfig

__all__ = [
    'TenantWiperConfig',
    'TenantDeleter',
]
