import logging
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base, relationship

from sqlalchemy_tenant_wiper.core import TenantDeleter, TenantWiperConfig, _parse_join_path

logging.getLogger().setLevel(logging.DEBUG)

# Test fixtures and models
@pytest.fixture
def mock_engine():
    """Create an in-memory SQLite database for testing."""
    return create_engine('sqlite:///:memory:')


@pytest.fixture
def test_base():
    """Create a test SQLAlchemy Base."""
    return declarative_base()


@pytest.fixture
def test_models(test_base):
    """Create test models for various scenarios."""

    class User(test_base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        tenant_id = Column(String(36))  # UUID as string
        org_id = Column(String(36))

    class Order(test_base):
        __tablename__ = 'orders'
        id = Column(Integer, primary_key=True)
        user_id = Column(Integer, ForeignKey('users.id'))
        tenant_id = Column(String(36))
        amount = Column(Integer)
        user = relationship('User')

    class Product(test_base):
        __tablename__ = 'products'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        # No tenant columns - needs relationship

    class ProductOrder(test_base):
        __tablename__ = 'product_orders'
        product_id = Column(Integer, ForeignKey('products.id'), primary_key=True)
        order_id = Column(Integer, ForeignKey('orders.id'), primary_key=True)
        quantity = Column(Integer)
        # Composite primary key example

    class AuditLog(test_base):
        __tablename__ = 'audit_logs'
        id = Column(Integer, primary_key=True)
        action = Column(String(50))
        # No tenant columns - excluded table

    # New models for testing multiple relationship paths
    class Company(test_base):
        __tablename__ = 'companies'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        tenant_id = Column(String(36))

    class Department(test_base):
        __tablename__ = 'departments'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        tenant_id = Column(String(36))

    class Employee(test_base):
        __tablename__ = 'employees'
        id = Column(Integer, primary_key=True)
        name = Column(String(100))
        company_id = Column(Integer, ForeignKey('companies.id'))
        department_id = Column(Integer, ForeignKey('departments.id'))
        # No tenant columns - has two paths to tenant data

    return {
        'User': User,
        'Order': Order,
        'Product': Product,
        'ProductOrder': ProductOrder,
        'AuditLog': AuditLog,
        'Company': Company,
        'Department': Department,
        'Employee': Employee
    }


@pytest.fixture
def tenant_data():
    """Create tenant IDs for testing."""
    return {
        'target_tenant_id': str(uuid4()),
        'target_org_id': str(uuid4()),
        'other_tenant_id': str(uuid4()),
        'other_org_id': str(uuid4())
    }


@pytest.fixture
def test_session(mock_engine, test_base, test_models, tenant_data):
    """Create a test session with populated data."""
    test_base.metadata.create_all(mock_engine)
    session = Session(mock_engine)

    target_tenant_id = tenant_data['target_tenant_id']
    target_org_id = tenant_data['target_org_id']
    other_tenant_id = tenant_data['other_tenant_id']
    other_org_id = tenant_data['other_org_id']

    # Users - mix of target tenant and other tenant
    user1 = test_models['User'](id=1, name='John', tenant_id=target_tenant_id, org_id=target_org_id)
    user2 = test_models['User'](id=2, name='Jane', tenant_id=target_tenant_id, org_id=target_org_id)
    user3 = test_models['User'](id=3, name='Bob', tenant_id=other_tenant_id, org_id=other_org_id)

    # Orders - some for target tenant, some for other
    order1 = test_models['Order'](id=1, user_id=1, tenant_id=target_tenant_id, amount=100)
    order2 = test_models['Order'](id=2, user_id=2, tenant_id=target_tenant_id, amount=200)
    order3 = test_models['Order'](id=3, user_id=3, tenant_id=other_tenant_id, amount=300)

    # Products (no tenant info - will be filtered via relationships)
    product1 = test_models['Product'](id=1, name='Widget')
    product2 = test_models['Product'](id=2, name='Gadget')
    product3 = test_models['Product'](id=3, name='Tool')

    # Product Orders (composite PK) - mix of tenant and non-tenant
    po1 = test_models['ProductOrder'](product_id=1, order_id=1, quantity=5)  # target tenant
    po2 = test_models['ProductOrder'](product_id=2, order_id=2, quantity=3)  # target tenant
    po3 = test_models['ProductOrder'](product_id=3, order_id=3, quantity=7)  # other tenant

    # Audit Logs (excluded from deletion)
    audit1 = test_models['AuditLog'](id=1, action='login')
    audit2 = test_models['AuditLog'](id=2, action='logout')

    # Test data for multiple relationship paths
    company1 = test_models['Company'](id=1, name='Target Corp', tenant_id=target_tenant_id)
    company2 = test_models['Company'](id=2, name='Other Corp', tenant_id=other_tenant_id)

    dept1 = test_models['Department'](id=1, name='Target Engineering', tenant_id=target_tenant_id)
    dept2 = test_models['Department'](id=2, name='Other Engineering', tenant_id=other_tenant_id)

    # Employees with different relationship paths to tenant
    # Both paths point to target tenant
    emp1 = test_models['Employee'](id=1, name='Alice', company_id=1, department_id=1)
    # Both paths point to other tenant
    emp2 = test_models['Employee'](id=2, name='Bob', company_id=2, department_id=2)
    # Mixed paths: target company, other department
    emp3 = test_models['Employee'](id=3, name='Charlie', company_id=1, department_id=2)

    session.add_all([
        user1, user2, user3,
        order1, order2, order3,
        product1, product2, product3,
        po1, po2, po3,
        audit1, audit2,
        company1, company2,
        dept1, dept2,
        emp1, emp2, emp3
    ])
    session.commit()

    return session, tenant_data


class TestTenantWiperConfig:
    """Test TenantWiperConfig initialization and validation."""

    def test_init_with_defaults(self, test_base):
        """Test config initialization with default values."""
        config = TenantWiperConfig(
            base=test_base,
            validate_on_init=False
        )

        assert config.base == test_base
        assert config.tenant_filters == []
        assert config.relationships == []
        assert config.excluded_tables == []
        assert config.validate_on_init is False
        assert config._relationship_dict == {}

    def test_init_with_tenant_filters(self, test_base):
        """Test config initialization with tenant filters."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
            lambda table: table.c.org_id.in_([str(test_uuid)])
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            validate_on_init=False
        )

        assert len(config.tenant_filters) == 2
        assert config.tenant_filters == tenant_filters

    def test_validate_success(self, test_base, test_models):
        """Test successful configuration validation."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        # Should not raise exception
        config.validate()

    def test_validate_fail_missing_table_coverage(self, test_base, test_models):
        """Test validation failure when table has no coverage."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                # Missing 'products' relationship - should fail
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match="The following tables lack the necessary tenant columns or a defined relationship path to tenant source .*'products'"):  # noqa
            config.validate()

    def test_validate_fail_malformed_relationship_syntax(self, test_base, test_models):
        """Test validation failure with malformed relationship syntax."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        # Test various malformed relationship syntaxes
        malformed_relationships = [
            'table1__fk=pk',  # Even number of parts
            'table1__fk_pk__table2',  # Missing '=' in condition
            'table1__fk=pk__',  # Incomplete join step
            'table1____table2',  # Empty condition
            '',  # Empty string
        ]

        for malformed_rel in malformed_relationships:
            config = TenantWiperConfig(
                base=test_base,
                tenant_filters=tenant_filters,
                tenant_join_paths=[malformed_rel],
                excluded_tables=['audit_logs', 'products', 'product_orders', 'employees', 'departments', 'companies'],
                validate_on_init=False
            )

            with pytest.raises(ValueError, match='Relationship path validation errors'):
                config.validate()

    def test_validate_fail_nonexistent_table_in_relationship(self, test_base, test_models):
        """Test validation failure with non-existent table in relationship."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'nonexistent_table__id=fk__orders',  # Non-existent source table
                'products__id=fk__nonexistent_target',  # Non-existent target table
            ],
            excluded_tables=['audit_logs', 'product_orders', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='does not exist in metadata'):
            config.validate()

    def test_validate_fail_nonexistent_column_in_relationship(self, test_base, test_models):
        """Test validation failure with non-existent column in relationship."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'products__nonexistent_col=id__orders',  # Non-existent from column
                'products__id=nonexistent_col__orders',  # Non-existent to column
            ],
            excluded_tables=['audit_logs', 'product_orders', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='does not exist in table'):
            config.validate()

    def test_validate_fail_relationship_to_table_without_tenant_filter(self, test_base, test_models):
        """Test validation failure when relationship path leads to table without tenant columns."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        # Create a relationship from product_orders to products (which has no tenant columns)
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'products__id=product_id__product_orders__order_id=id__orders',
                'product_orders__product_id=id__products',  # Leads to products (no tenant filter)
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='cannot be filtered by any tenant filters'):
            config.validate()

    def test_validate_fail_table_in_both_excluded_and_relationships(self, test_base, test_models):
        """Test validation failure when table is in both excluded and relationships."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'audit_logs__user_id=id__users',  # audit_logs is also in excluded
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],  # Same table in excluded
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='Configuration Error.*excluded_tables.*relationships'):
            config.validate()

    def test_validate_fail_circular_relationship_detection(self, test_base, test_models):
        """Test validation handles potential circular relationships."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        # Create a potential circular reference (though our current models don't support this)
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'products__id=product_id__product_orders__order_id=user_id__users',  # Wrong column
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='does not exist in table'):
            config.validate()

    def test_validate_fail_complex_multi_hop_with_wrong_column(self, test_base, test_models):
        """Test validation failure in complex multi-hop relationship with wrong column."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                # Complex path with wrong column in the middle
                'products__id=product_id__product_orders__wrong_column=id__orders',
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match="Column 'wrong_column' does not exist"):
            config.validate()

    def test_validate_on_init_parameter(self, test_base, test_models):
        """Test that validate_on_init=True actually validates during __init__."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        # This should fail during __init__ because validate_on_init=True (default)
        with pytest.raises(ValueError, match='The following tables lack the necessary tenant columns or a defined relationship path to tenant source '):  # noqa
            TenantWiperConfig(
                base=test_base,
                tenant_filters=tenant_filters,
                tenant_join_paths=[
                    'product_orders__order_id=id__orders',
                    # Missing 'products' relationship
                ],
                excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
                validate_on_init=True  # Explicit True
            )

    def test_validate_edge_case_empty_relationships_and_filters(self, test_base, test_models):
        """Test validation with completely empty configuration."""
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[],  # No filters
            tenant_join_paths=[],   # No relationships
            excluded_tables=[], # No exclusions
            validate_on_init=False
        )

        # Should fail because no tables have coverage
        with pytest.raises(ValueError, match='The following tables lack the necessary tenant columns or a defined relationship path to tenant source '):  # noqa
            config.validate()

    def test_validate_fail_relationship_final_table_column_check(self, test_base, test_models):
        """Test validation properly checks final table can be filtered by tenant filters."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),  # Only looks for tenant_id
        ]

        # This should fail because products table has no tenant_id column
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'product_orders__product_id=id__products',  # Final table 'products' has no tenant_id
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='cannot be filtered by any tenant filters.*tenant_id'):
            config.validate()

    def test_validate_success_relationship_final_table_column_check(self, test_base, test_models):
        """Test validation passes when final table can be filtered by tenant filters."""
        test_uuid = uuid4()
        tenant_filters = [
            lambda table: table.c.tenant_id == str(test_uuid),
        ]

        # This should succeed because orders table has tenant_id column
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=tenant_filters,
            tenant_join_paths=[
                'product_orders__order_id=id__orders',  # Final table 'orders' has tenant_id
                'products__id=product_id__product_orders__order_id=id__orders'  # Final table 'orders' has tenant_id
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        # Should not raise exception
        config.validate()

    def test_validate_tenant_filter_syntax_error(self, test_base, test_models):
        """Test that tenant filter syntax errors are properly distinguished from missing column errors."""
        test_uuid = uuid4()

        # Filter with syntax error - trying to call invalid method
        def syntax_error_filter(table):
            return table.c.tenant_id.invalid_method()

        # Filter that accesses non-existent column
        def missing_column_filter(table):
            return table.c.nonexistent_column == str(test_uuid)

        # Filter that works correctly
        def valid_filter(table):
            return table.c.tenant_id == str(test_uuid)

        # Test syntax error is caught and raised
        config_syntax_error = TenantWiperConfig(
            base=test_base,
            tenant_filters=[syntax_error_filter],
            excluded_tables=['audit_logs', 'products', 'product_orders', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='Error on applying tenant filter'):
            config_syntax_error.validate()

        # Test missing column is handled gracefully (no error, just no coverage)
        config_missing_column = TenantWiperConfig(
            base=test_base,
            tenant_filters=[missing_column_filter],
            excluded_tables=['audit_logs', 'products', 'product_orders', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        # Should fail with coverage error (not syntax error)
        with pytest.raises(ValueError, match='The following tables lack the necessary tenant columns or a defined relationship path to tenant source '):   #noqa
            config_missing_column.validate()

        # Test valid filter works
        config_valid = TenantWiperConfig(
            base=test_base,
            tenant_filters=[valid_filter],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        # Should not raise exception
        config_valid.validate()

    def test_validate_tenant_filter_error_on_expression_execute(self, test_session, test_base, test_models):
        """Test tenant filter with error on compile/execution time"""
        test_uuid = uuid4()

        # Filter with syntax error - trying to call invalid method
        def wrong_expression_compile(table):
            return table.c.tenant_id == test_uuid  # different field type to compare

        # Test syntax error is caught and raised
        config_syntax_error = TenantWiperConfig(
            base=test_base,
            tenant_filters=[wrong_expression_compile],
            excluded_tables=['audit_logs', 'products', 'product_orders', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        # it should pass
        config_syntax_error.validate()

        deleter = TenantDeleter(config_syntax_error)
        session, _ = test_session
        initial_user_count = session.query(test_models['User']).count()
        initial_order_count = session.query(test_models['Order']).count()

        # Execute dry run
        with pytest.raises(Exception, match='Error binding parameter'):
            deleter.delete(session, dry_run=True, commit=False)

        # Verify nothing was actually deleted
        final_user_count = session.query(test_models['User']).count()
        final_order_count = session.query(test_models['Order']).count()

        assert final_user_count == initial_user_count
        assert final_order_count == initial_order_count

        # PKs should not be collected
        assert len(deleter.pks_to_delete) == 0


    def test_validate_relationship_path_tenant_filter_syntax_error(self, test_base, test_models):
        """Test that syntax errors in tenant filters are caught during relationship validation."""
        # Filter with syntax error
        def syntax_error_filter(table):
            return table.c.tenant_id.invalid_method()

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[syntax_error_filter],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',  # Should fail when validating final table
            ],
            excluded_tables=['audit_logs', 'products', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        with pytest.raises(ValueError, match='Error on applying tenant filter'):
            config.validate()


class TestJoinPathParsing:
    """Test relationship path parsing functionality."""

    def test_parse_simple_join_path(self):
        """Test parsing a simple two-table join path."""
        path = 'table1__fk=pk__table2'
        result = _parse_join_path(path)

        expected = {
            'start_table': 'table1',
            'final_table': 'table2',
            'steps': [{
                'from_table': 'table1',
                'from_key': 'fk',
                'to_table': 'table2',
                'to_key': 'pk'
            }]
        }

        assert result == expected

    def test_parse_complex_join_path(self):
        """Test parsing a complex multi-table join path."""
        path = 'table1__fk1=pk1__table2__fk2=pk2__table3'
        result = _parse_join_path(path)

        expected = {
            'start_table': 'table1',
            'final_table': 'table3',
            'steps': [
                {
                    'from_table': 'table1',
                    'from_key': 'fk1',
                    'to_table': 'table2',
                    'to_key': 'pk1'
                },
                {
                    'from_table': 'table2',
                    'from_key': 'fk2',
                    'to_table': 'table3',
                    'to_key': 'pk2'
                }
            ]
        }

        assert result == expected


class TestTenantDeleter:
    """Test TenantDeleter functionality with real data."""

    def test_init(self, test_base):
        """Test TenantDeleter initialization."""
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)

        assert deleter.config == config
        assert deleter.metadata == test_base.metadata
        assert deleter.excluded_tables == set()
        assert deleter.pks_to_delete == {}

    def test_build_deletion_order(self, test_base, test_models):
        """Test deletion order respects foreign key dependencies."""
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)
        deletion_order = deleter._build_deletion_order()

        # Should be reversed metadata.sorted_tables
        assert deletion_order == list(reversed(test_base.metadata.sorted_tables))


class TestRealDataScenarios:
    """Test scenarios with real data insertion and deletion."""


    def test_tenant_deletion_with_real_data(self, test_session, test_base, test_models):
        """Test complete tenant deletion workflow with real data."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']
        other_tenant_id = tenant_data['other_tenant_id']

        # Verify initial data exists
        all_users = session.query(test_models['User']).all()
        all_orders = session.query(test_models['Order']).all()
        all_audits = session.query(test_models['AuditLog']).all()

        assert len(all_users) == 3  # 2 target + 1 other
        assert len(all_orders) == 3  # 2 target + 1 other
        assert len(all_audits) == 2  # Should remain after deletion

        # Count target tenant data before deletion
        target_users = session.query(test_models['User']).filter(
            test_models['User'].tenant_id == target_tenant_id
        ).all()
        target_orders = session.query(test_models['Order']).filter(
            test_models['Order'].tenant_id == target_tenant_id
        ).all()

        assert len(target_users) == 2
        assert len(target_orders) == 2

        # Create deletion config - need relationships for indirect tables
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)

        # Execute deletion
        deleter.delete(session, dry_run=False, commit=True)

        # Verify target tenant data is deleted
        remaining_users = session.query(test_models['User']).all()
        remaining_orders = session.query(test_models['Order']).all()
        remaining_audits = session.query(test_models['AuditLog']).all()

        # Should only have other tenant data remaining
        assert len(remaining_users) == 1
        assert remaining_users[0].tenant_id == other_tenant_id

        assert len(remaining_orders) == 1
        assert remaining_orders[0].tenant_id == other_tenant_id

        # Audit logs should be untouched (excluded)
        assert len(remaining_audits) == 2

    def test_tenant_deletion_direct_no_join_explicit(self, test_session, test_base, test_models):
        """Test complete tenant deletion workflow with direct reference."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']
        other_tenant_id = tenant_data['other_tenant_id']

        # Create deletion config - need relationships for indirect tables
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'orders',
                'orders__user_id=id__users',
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies',
                             'products', 'product_orders'],
            validate_on_init=True
        )

        deleter = TenantDeleter(config)

        # Execute deletion
        deleter.delete(session, dry_run=False, commit=True)

        # Verify target tenant data is deleted
        remaining_users = session.query(test_models['User']).all()

        # Should only have other tenant data remaining
        assert len(remaining_users) == 1
        assert remaining_users[0].tenant_id == other_tenant_id


    def test_dry_run_reports_correctly(self, test_session, test_base, test_models):
        """Test dry run reports what would be deleted without actually deleting."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        # Count before dry run
        initial_user_count = session.query(test_models['User']).count()
        initial_order_count = session.query(test_models['Order']).count()

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)

        # Execute dry run
        deleter.delete(session, dry_run=True, commit=False)

        # Verify nothing was actually deleted
        final_user_count = session.query(test_models['User']).count()
        final_order_count = session.query(test_models['Order']).count()

        assert final_user_count == initial_user_count
        assert final_order_count == initial_order_count

        # But PKs should have been collected
        assert len(deleter.pks_to_delete) > 0

    def test_relationship_based_deletion(self, test_session, test_base, test_models):
        """Test deletion of tables connected via relationships."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        # Verify initial product orders exist
        all_product_orders = session.query(test_models['ProductOrder']).all()
        assert len(all_product_orders) == 3

        # Count target tenant product orders (via relationship to orders)
        target_product_orders = session.query(test_models['ProductOrder']).join(
            test_models['Order']
        ).filter(test_models['Order'].tenant_id == target_tenant_id).all()
        assert len(target_product_orders) == 2

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders'
            ],
            # Keep products for this test
            excluded_tables=['audit_logs', 'products', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)
        deleter.delete(session, dry_run=False, commit=True)

        # Verify only target tenant product orders were deleted
        remaining_product_orders = session.query(test_models['ProductOrder']).all()
        assert len(remaining_product_orders) == 1

        # Verify the remaining one belongs to other tenant
        remaining_po = remaining_product_orders[0]
        related_order = session.query(test_models['Order']).filter(
            test_models['Order'].id == remaining_po.order_id
        ).first()
        assert related_order.tenant_id != target_tenant_id

    def test_composite_primary_key_deletion(self, test_session, test_base, test_models):
        """Test deletion of tables with composite primary keys."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'products', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)

        # Test PK collection for composite key table
        product_orders_table = test_models['ProductOrder'].__table__
        query = deleter._build_pk_collection_query(product_orders_table)

        assert query is not None
        # Should select both columns of composite key
        assert len(query.selected_columns) == 2

        # Execute and verify composite key handling
        deleter.delete(session, dry_run=False, commit=True)

        # Verify deletion worked correctly
        remaining_pos = session.query(test_models['ProductOrder']).all()
        assert len(remaining_pos) == 1

    def test_multiple_tenant_filters(self, test_session, test_base, test_models):
        """Test deletion with multiple tenant filters (OR logic)."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']
        target_org_id = tenant_data['target_org_id']

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id,
                lambda table: table.c.org_id == target_org_id,
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)
        deleter.delete(session, dry_run=False, commit=True)

        # Should delete all users/orders with either matching tenant_id OR org_id
        remaining_users = session.query(test_models['User']).all()
        remaining_orders = session.query(test_models['Order']).all()

        # Only users/orders with different tenant_id AND different org_id should remain
        for user in remaining_users:
            assert user.tenant_id != target_tenant_id
            assert user.org_id != target_org_id

        for order in remaining_orders:
            assert order.tenant_id != target_tenant_id

    def test_excluded_tables_not_touched(self, test_session, test_base, test_models):
        """Test that excluded tables are never touched."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        initial_audit_count = session.query(test_models['AuditLog']).count()

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)
        deleter.delete(session, dry_run=False, commit=True)

        # Audit logs should be completely untouched
        final_audit_count = session.query(test_models['AuditLog']).count()
        assert final_audit_count == initial_audit_count

        # And should not appear in pks_to_delete
        assert 'audit_logs' not in deleter.pks_to_delete

    def test_error_handling_with_rollback(self, test_session, test_base, test_models):
        """Test error handling and rollback functionality."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        initial_user_count = session.query(test_models['User']).count()

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)

        # Force an error during execution by mocking session.execute to fail
        with patch.object(session, 'execute') as mock_execute:
            mock_execute.side_effect = Exception('Database error')

            with pytest.raises(Exception, match='Database error'):
                deleter.delete(session, dry_run=False, commit=True)

        # Verify rollback occurred - data should be unchanged
        final_user_count = session.query(test_models['User']).count()
        assert final_user_count == initial_user_count


class TestMockedScenarios:
    """Test scenarios that benefit from mocking."""

    @patch('sqlalchemy_tenant_wiper.core.logger')
    def test_collect_pks_logging(self, mock_logger, test_session, test_base):
        """Test that PK collection logs appropriately."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'product_orders__order_id=id__orders',
                'products__id=product_id__product_orders__order_id=id__orders'
            ],
            excluded_tables=['audit_logs', 'employees', 'departments', 'companies'],
            validate_on_init=False
        )

        deleter = TenantDeleter(config)
        deleter.delete(session, dry_run=True, commit=False)

        # Verify appropriate logging occurred
        mock_logger.info.assert_called()
        log_calls = [call.args[0] for call in mock_logger.info.call_args_list]

        # Should log phase start/end and table processing
        phase_logs = [log for log in log_calls if 'Phase' in log]
        assert len(phase_logs) >= 2  # At least start and end

        collect_logs = [log for log in log_calls if 'Collect' in log]
        assert len(collect_logs) > 0  # Should log collection for each table


class TestMultipleRelationshipPaths:
    """Test scenarios with multiple relationship paths to the same table."""

    def test_table_with_multiple_relationship_paths_current_behavior(self, test_session, test_base, test_models):
        """Test current behavior - both path are used."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        # Configure with two different relationship paths to employees table
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'employees__company_id=id__companies',
                'employees__department_id=id__departments'
            ],
            excluded_tables=['audit_logs', 'users', 'orders', 'products', 'product_orders'],
            validate_on_init=False
        )

        # Both paths should be valid, but only one table will be stored
        assert len(config._relationship_dict) == 1
        assert config._relationship_dict['employees'] == [
            'employees__company_id=id__companies',
            'employees__department_id=id__departments'
        ]

        deleter = TenantDeleter(config)
        deleter.delete(session, dry_run=False, commit=True)

        remaining_employees = session.query(test_models['Employee']).all()

        # So only employees with both other_tenant_id department and companies should remain
        assert len(remaining_employees) == 1

    def test_table_with_multiple_relationship_paths_expected_behavior(self, test_session, test_base, test_models):
        """Test expected behavior - both paths should be considered (OR logic)."""
        session, tenant_data = test_session
        target_tenant_id = tenant_data['target_tenant_id']

        # This test demonstrates what the expected behavior SHOULD be
        # but will currently fail due to the bug

        # Manually test what should happen with both paths
        # Path 1: employees with target companies should be deleted
        target_company_employees = session.query(test_models['Employee']).join(
            test_models['Company']
        ).filter(test_models['Company'].tenant_id == target_tenant_id).all()

        # Path 2: employees with target departments should be deleted
        target_dept_employees = session.query(test_models['Employee']).join(
            test_models['Department']
        ).filter(test_models['Department'].tenant_id == target_tenant_id).all()

        # Expected: employees reachable via either path should be deleted
        # emp1: reachable via both company and department (should be deleted)
        # emp2: not reachable via either path (should remain)
        # emp3: reachable via company path only (should be deleted)

        expected_to_delete = set()
        expected_to_delete.update(emp.id for emp in target_company_employees)
        expected_to_delete.update(emp.id for emp in target_dept_employees)

        # Should delete emp1 and emp3 (both reachable via at least one path)
        assert expected_to_delete == {1, 3}

        # This documents the expected behavior that should be implemented
        # Currently this would fail because only one path is used

    def test_validation_passes_with_multiple_paths(self, test_base, test_models):
        """Test that validation passes when multiple paths exist."""
        target_tenant_id = str(uuid4())

        # Both paths should be valid during validation
        config = TenantWiperConfig(
            base=test_base,
            tenant_filters=[
                lambda table: table.c.tenant_id == target_tenant_id
            ],
            tenant_join_paths=[
                'employees__company_id=id__companies',     # Valid path
                'employees__department_id=id__departments'  # Also valid path
            ],
            excluded_tables=['audit_logs', 'users', 'orders', 'products', 'product_orders'],
            validate_on_init=False
        )

        # Should not raise exception during validation
        config.validate()

        # But only one path is actually stored
        assert len(config._relationship_dict) == 1


if __name__ == '__main__':
    pytest.main([__file__])
