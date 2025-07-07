# SQLAlchemy Tenant Wiper

A flexible SQLAlchemy-based library for tenant data deletion in multi-tenant applications. Supports custom dynamic tenant column filtering using lambda expressions, robust validations and handles complex relationship paths for tables without explicit tenant knowledge

[![PyPI version](https://badge.fury.io/py/sqlalchemy-tenant-wiper.svg)](https://badge.fury.io/py/sqlalchemy-tenant-wiper)
[![Python Support](https://img.shields.io/pypi/pyversions/sqlalchemy-tenant-wiper.svg)](https://pypi.org/project/sqlalchemy-tenant-wiper/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Dynamic Tenant Filtering**: Lambda expressions adapt to each table's columns
- **Relationship Path Support**: Declare relationships for tables that indirectly belong to a tenants (See below)
- **Composite Primary Key Support**: Properly handles tables with composite primary keys
- **Configuration-Time Validation**: Comprehensive runtime validation at table & column level
- **Two-Phase Deletion**: Safe deletion order respecting foreign key constraints
- **Dry Run Mode**: Preview what would be deleted before execution
- **Batched Deletions**: Efficient deletion in configurable batch sizes

## Installation

### Using pip

```bash
pip install sqlalchemy-tenant-wiper
```

### Using Poetry

```bash
poetry add sqlalchemy-tenant-wiper
```

## Quick Start

```python
from sqlalchemy_tenant_wiper import TenantWiperConfig, TenantDeleter
from uuid import UUID

# Configure your tenant deletion settings
config = TenantWiperConfig(
    base=YourSQLAlchemyBase,
    tenant_filters=[
        # A record is targeted for deletion if it matches *any* of these simple conditions you provide.
        lambda table: table.c.tenant_id == your_tenant_uuid,
        lambda table: table.c.org_id.in_(your_org_uuids)
    ],
    tenant_join_paths=[
        # tables with 'tenant_id' or 'org_id' column doesn't require declaration 
        # and explicitly added for removal

        # <table_name>__<from_key>=<to_key>__<to_table_name>...
        'products__id=product_id__order_items__order_id=id__orders'
    ],
    excluded_tables=['audit_logs', 'system_logs'],
    batch_size=500
)

# Create deleter and execute
deleter = TenantDeleter(config)
deleter.delete(session, dry_run=True)  # Preview first
deleter.delete(session, commit=True)   # Execute deletion
```

## Usage Examples

### Basic Tenant Filtering

```python
from sqlalchemy_tenant_wiper import TenantWiperConfig, TenantDeleter
from uuid import UUID

# Simple tenant filtering by tenant_id
tenant_uuid = UUID('12345678-1234-5678-9012-123456789abc')

config = TenantWiperConfig(
    base=Base,
    tenant_filters=[
        lambda table: table.c.tenant_id == str(tenant_uuid)
    ]
)

deleter = TenantDeleter(config)
deleter.delete(session, dry_run=True, commit=False)
```

### Multiple Tenant Filters

```python
# Multiple ways to identify tenant data
config = TenantWiperConfig(
    base=Base,
    tenant_filters=[
        lambda table: table.c.tenant_id == str(tenant_uuid),
        lambda table: table.c.organization_id.in_(org_uuids),
        lambda table: table.c.customer_code == 'ACME_CORP'
    ]
)
```

### Relationship Paths for Indirect Tables

```python
# Handle tables without direct tenant columns
config = TenantWiperConfig(
    base=Base,
    tenant_filters=[
        lambda table: table.c.tenant_id == str(tenant_uuid)
    ],
    tenant_join_paths=[
        # <table_name>__<from_key>=<to_key>__<to_table_name>...
        # Products -> OrderItems -> Orders (with tenant_id)
        'products__id=product_id__order_items__order_id=id__orders',
        
        # Audit logs -> Users (with tenant_id)  
        'audit_logs__user_id=id__users',
        
        # Complex multi-hop relationship
        'categories__id=category_id__products__id=product_id__order_items__order_id=id__orders'
    ]
)
```

### Configuration Validation

```python
# Validate configuration at startup/in tests
config = TenantWiperConfig(
    base=Base,
    tenant_filters=[...],
    tenant_join_paths=[...],
    validate_on_init=True  # Default: validates on creation
)

# Or validate explicitly
config = TenantWiperConfig(base=Base, validate_on_init=False)
config.validate()  # Call when ready
```

### Excluding Tables

```python
# Skip certain tables from deletion
config = TenantWiperConfig(
    base=Base,
    tenant_filters=[...],
    excluded_tables=[
        'audit_logs',
        'system_configs', 
        'migration_history'
    ]
)
```

## Configuration Reference

### TenantWiperConfig

| Parameter | Type | Description |
|-----------|------|-------------|
| `base` | SQLAlchemy Base | Your declarative base class |
| `tenant_filters` | `List[Callable[[Table], Any]]` | Lambda functions for tenant filtering |
| `tenant_join_paths` | `List[str]` | Relationship path strings for indirect tables |
| `excluded_tables` | `List[str]` | Table names to exclude from deletion |
| `validate_on_init` | `bool` | Whether to validate config on creation (default: True) |

### TenantDeleter.delete()

| Parameter | Type | Description |
|-----------|------|-------------|
| `session` | SQLAlchemy Session | Database session for operations |
| `dry_run` | `bool` | If True, only report what would be deleted |
| `commit` | `bool` | If True, commit the transaction |

## Relationship Path Syntax

Relationship paths use double underscore (`__`) separators:

```
table1__from_key=to_key__table2__from_key2=to_key2__table3
```

Example:
```
products__id=product_id__order_items__order_id=id__orders
```

This creates joins:
1. `products.id = order_items.product_id`
2. `order_items.order_id = orders.id`

The final table (`orders`) must have tenant filter columns.

## Requirements

- Python 3.8+
- SQLAlchemy 1.4+

## Development

### Setup

```bash
git clone https://github.com/yourusername/sqlalchemy-tenant-wiper.git
cd sqlalchemy-tenant-wiper
poetry install
```

### Testing

```bash
poetry run pytest
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.


## TODO
[ ] Make enforcing listing of all tables as optional 