import inspect
import itertools
import logging
import pprint
import traceback
from collections import defaultdict
from time import perf_counter
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from unittest.mock import Mock

from sqlalchemy import literal, or_, select, tuple_
from sqlalchemy.orm import Session
from sqlalchemy.schema import Table

logger = logging.getLogger(__name__)

class ColumnRecorder:
    """Records column access for tenant filter validation."""

    def __init__(self):
        self.accessed_columns = set()

    def __getattr__(self, column_name):
        self.accessed_columns.add(column_name)
        return Mock()  # Return mock for any method calls (in_, ==, etc.)

class TableProxy:
    """
    A proxy for a SQLAlchemy Table.

    It intercepts access to the '.c' attribute to return a ColumnRecorder,
    but passes all other attribute access through to the real table.
    """
    def __init__(self, real_table):
        self._real_table = real_table
        # Instead of replacing .c on the real table, we create our own .c
        # that points to our recorder.
        self.c = ColumnRecorder()

    def __getattr__(self, name):
        return getattr(self._real_table, name)

    def __repr__(self):
        return f'<TableProxy for {self._real_table.name}>'


class TenantWiperConfig:
    """Configuration for tenant data wiping with flexible Base and filtering."""

    def __init__(
        self,
        base,
        tenant_filters: Optional[List[Callable[[Table], Any]]] = None,
        tenant_join_paths: Optional[List[str]] = None,
        excluded_tables: Optional[List[str]] = None,
        validate_on_init: bool = True,
        batch_size: Optional[int] = 500
    ):
        """
        Initialize tenant wiper configuration.

        Args:
            base: SQLAlchemy declarative Base
            tenant_filters: List of lambda expressions for tenant filtering
            tenant_join_paths:  Defines explicit join paths for tables that indirectly belong to a tenant.
            excluded_tables: List of table names to exclude from deletion
            validate_on_init: Whether to validate configuration on initialization
        """
        self.base = base
        self.tenant_filters = tenant_filters if tenant_filters is not None else []
        self.relationships = tenant_join_paths if tenant_join_paths is not None else []
        self.excluded_tables = excluded_tables if excluded_tables is not None else []
        self.validate_on_init = validate_on_init
        self.batch_size = batch_size

        # Parse relationships into lookup dict
        self._relationship_dict: Dict[str, List[str]] = self._parse_relationships()

        if self.validate_on_init:
            self.validate()

    def _parse_relationships(self) -> Dict[str, List[str]]:
        """Parse relationship config into lookup dict with multiple paths per table."""
        relationship_dict = {}
        for relationship_str in self.relationships:
            source_table = relationship_str.split('__', 1)[0]
            if source_table not in relationship_dict:
                relationship_dict[source_table] = []
            relationship_dict[source_table].append(relationship_str)
        return relationship_dict


    def validate(self) -> None:
        """Validate configuration for correctness."""
        logger.info('[Tenant Wiper] Validating table declarations and configuration...')
        metadata = self.base.metadata
        excluded_tables_set = set(self.excluded_tables)
        relationship_errors = []

        # Validate relationship paths
        for source_table, relationship_paths in self._relationship_dict.items():
            if source_table in excluded_tables_set:
                relationship_errors.append(
                    f"Configuration Error: Table '{source_table}' is listed in both "
                    f"excluded_tables and relationships."
                )
                continue

            # Validate all paths for this table
            for relationship_path in relationship_paths:
                path_errors = _validate_relationship_path(relationship_path, metadata, self.tenant_filters)
                if path_errors:
                    relationship_errors.extend([
                        f"Relationship '{source_table}' path '{relationship_path}': {error}"
                        for error in path_errors
                    ])
        if relationship_errors:
            error_msg = 'Relationship path validation errors:\n' + '\n'.join(relationship_errors)
            raise ValueError(error_msg)

        # Validate table coverage
        tables_with_no_coverage = []
        sorted_tables = metadata.sorted_tables
        implicit_direct_relationships = 0

        for table in sorted_tables:
            table_name = table.name
            if table_name in excluded_tables_set:
                logging.info(f'[Tenant Wiper] Skipped "{table_name}" because in excluded table set')
                continue

            has_tenant_filter = self._has_tenant_column(table)
            if has_tenant_filter:
                implicit_direct_relationships += 1

            if has_tenant_filter and table_name in self._relationship_dict:
                logger.warning(
                    f"Table '{table_name}' can be directly filtered by tenant filters "
                    f"but also has an entry in relationships. "
                    f"Please ensure this is intentional."
                )

            if not has_tenant_filter and table_name not in self._relationship_dict:
                tables_with_no_coverage.append(table_name)

        if tables_with_no_coverage:
            error_msg = f'Cannot apply tenant filter: The following tables lack the necessary tenant columns'\
                        f' or a defined relationship path to tenant source (e.g., "table__from_pk=to_pk__tenantsrc): {tables_with_no_coverage}'  # noqa
            pprint.pprint(self._relationship_dict)
            raise ValueError(error_msg)

        logger.info(
            f'Tenant wiper validation passed:\n  {len(metadata.sorted_tables)} tables checked. '
            f'\n  {len(self._relationship_dict)} table relationships validated. '
            f'\n  {implicit_direct_relationships} tables have direct tenant filters. '
            f'\n  {len(excluded_tables_set)} tables explicitly excluded.'
        )

    def _has_tenant_column(self, table: Table) -> bool:
        """Check if any tenant filter can be applied to this table."""
        for tenant_filter in self.tenant_filters:
            try:
                can_apply, _ = _can_apply_tenant_filter(table, tenant_filter)
                if can_apply:
                    return True
            except ValueError:
                # Syntax error - re-raise to surface the issue
                raise
        return False


def _parse_join_path(path: str) -> Dict[str, Any]:
    """
    Parses a string like 'table1__fk=pk__table2__fk2=pk2__table3'
    into a structured dictionary containing the start table and join steps.
    """
    parts = path.split('__')
    if len(parts) % 2 == 0:
        raise ValueError(f"Malformed join path '{path}': Must have an odd number of parts.")

    start_table = parts[0]
    join_steps = []

    iterator = iter(parts[1:])
    step_pairs = itertools.zip_longest(iterator, iterator)

    from_table_name = start_table
    for condition, to_table_name in step_pairs:
        if condition is None or to_table_name is None:
            raise ValueError(f"Incomplete join step in path '{path}'")

        try:
            from_key, to_key = condition.split('=')
        except ValueError:
            raise ValueError(
                f"Invalid join condition format '{condition}' in path '{path}'. Expected 'from_key=to_key'."
            )

        join_steps.append({
            'from_table': from_table_name,
            'from_key': from_key,
            'to_table': to_table_name,
            'to_key': to_key,
        })
        from_table_name = to_table_name

    return {
        'start_table': start_table,
        'final_table': from_table_name,
        'steps': join_steps
    }


def _get_model_class_for_table(table_name: str, base) -> Optional[type]:
    """
    Find the SQLAlchemy model class for a given table name by looking through Base registry.
    Returns None if no model class is found.
    """
    try:
        if hasattr(base, 'registry'):
            for mapper in base.registry.mappers:
                if mapper.class_.__tablename__ == table_name:
                    return mapper.class_
    except (AttributeError, KeyError):
        pass
    return None


def _get_all_columns_for_table(table_name: str, metadata, base) -> Set[str]:
    """
    Get all columns for a table, including inherited columns from model classes.
    Falls back to metadata columns if no model class is found.
    """
    model_class = _get_model_class_for_table(table_name, base)
    if model_class:
        try:
            mapper = model_class.__mapper__
            return set(mapper.columns.keys())
        except AttributeError:
            pass

    if table_name in metadata.tables:
        return set(metadata.tables[table_name].columns.keys())

    return set()


def _can_apply_tenant_filter(table: Table, tenant_filter: Callable[[Table], Any]) -> Tuple[bool, Set[str]]:
    """
    Check if table can be filtered by the given tenant filter directly.

    Returns:
        bool, Set[str]: True if filter can be applied, and a set of accessed columns.

    Raises:
        ValueError: Filter has syntax/expression errors
    """
    # Record column access with mock
    recorder = ColumnRecorder()
    mock_table = TableProxy(table)
    mock_table.c = recorder

    try:
        tenant_filter(mock_table)
    except Exception as e:
        # Even mock failed - syntax error
        raise ValueError(f'Filter syntax error: {e}')

    # Check if accessed columns exist in real table
    missing_columns = [col for col in recorder.accessed_columns
                     if col not in table.c]
    if missing_columns:
        return False, recorder.accessed_columns  # Table doesn't have required columns

    # If we have column, try another validation to ensure safe execution pre deletion
    try:
        filter_expression = tenant_filter(table)
        dummy_query = select(literal(1)).select_from(table).where(filter_expression)
        logger.debug(f"[Tenant Wiper] [Filter] '{table.name}' compiled filter: {filter_expression} sql: {dummy_query}")
        # We use a generic dialect for this.
        dummy_query.compile()
        # we validated what we could here, maybe add session execute() to ensure it works in context
        return True, recorder.accessed_columns
    except Exception as e:
        try:
            # lambda/fn source code for better error reporting
            filter_source = inspect.getsource(tenant_filter).strip()
        except TypeError:
            filter_source = '<Unknown tenant filter function>'

        raise ValueError(f"Table '{table.name}': Error on applying tenant filter:\n{filter_source}. \nPlease check your tenant filter table compatibility or fix it: {e}")  # noqa


def _validate_relationship_path(relationship_path: str, metadata,
                                tenant_filters: Optional[List[Callable[[Table], Any]]]=None) -> List[str]:
    """
    Validate that all tables and columns referenced in a relationship path actually exist.
    Returns list of validation errors, empty list if valid.
    """
    if not relationship_path:
        return ['Empty relationship path provided']
    validation_time_start = perf_counter()
    errors = []
    metadata_tables = metadata.tables
    try:
        parsed_path = _parse_join_path(relationship_path)
    except ValueError as e:
        traceback.print_exc()
        logger.error(f"Error parsing relationship path '{relationship_path}': {e}")
        return [str(e)]

    for step in parsed_path['steps']:
        from_table, from_key = step['from_table'], step['from_key']
        to_table, to_key = step['to_table'], step['to_key']

        # Check tables
        for table_name in [from_table, to_table]:
            if table_name not in metadata_tables:
                errors.append(f"Table '{table_name}' does not exist in metadata")
        if errors:
            continue

        # Check columns
        from_cols = _get_all_columns_for_table(from_table, metadata, None)
        if from_key not in from_cols:
            errors.append(f"Column '{from_key}' does not exist in table '{from_table}'")

        to_cols = _get_all_columns_for_table(to_table, metadata, None)
        if to_key not in to_cols:
            errors.append(f"Column '{to_key}' does not exist in table '{to_table}'")

    # Validate that the final table can be filtered by tenant filters
    if not errors and tenant_filters:
        final_table_name = parsed_path['final_table']
        final_table = metadata_tables[final_table_name]

        # Check if any tenant filter can be applied to the final table
        accessed_columns_pairs = list()
        can_filter_final_table = False
        for tenant_filter in tenant_filters:
            try:
                can_apply, accessed_columns = _can_apply_tenant_filter(final_table, tenant_filter)
                accessed_columns_pairs.append(list(accessed_columns))  # Use frozenset for immutability in set
                if can_apply:
                    can_filter_final_table = True
                    break
            except ValueError:
                # Syntax error - re-raise to surface the issue
                raise

        if not can_filter_final_table:
            errors.append(
                f"Final table '{final_table_name}' in path '{relationship_path}' "
                f"cannot be filtered by any tenant filters. The final table in a relationship "
                f"path must have columns ({' or '.join(map(str, accessed_columns_pairs))}) "
                f"that match the tenant filters. Available columns: {final_table.columns.keys()}"
            )

    end_time = perf_counter()
    logger.debug(f'[Tenant Wiper] Validation time for {relationship_path} took {end_time - validation_time_start:.4f} seconds')  # noqa
    return errors


class TenantDeleter:
    """Main class for tenant data deletion using flexible configuration."""

    def __init__(self, config: TenantWiperConfig):
        """Initialize with a TenantWiperConfig."""
        self.config = config
        self.metadata = config.base.metadata
        self.excluded_tables = set(config.excluded_tables)
        self.pks_to_delete: Dict[str, Set[Any]] = defaultdict(set)

    def _build_deletion_order(self) -> List[Table]:
        """Returns tables sorted for safe deletion (dependencies first)."""
        return list(reversed(self.metadata.sorted_tables))

    def _build_pk_collection_query(self, table: Table) -> Optional[Any]:
        """
        Builds a query to SELECT the primary keys of rows to be deleted for a given table.
        """
        if not table.primary_key:
            logger.warning(f"Table '{table.name}' has no primary key, cannot collect PKs.")
            raise ValueError(f"Table '{table.name}' has no primary key, cannot collect PKs.")

        primary_key_columns = list(table.primary_key.columns)
        # For composite primary keys, select all columns; for single, just the one
        if len(primary_key_columns) == 1:
            primary_selection = primary_key_columns[0]
        else:
            # For composite keys, select all primary key columns as tuple
            primary_selection = primary_key_columns

        # Case 1: Table can be filtered by tenant filters
        applicable_filters = []
        for tenant_filter in self.config.tenant_filters:
            try:
                filter_expr = tenant_filter(table)
                applicable_filters.append(filter_expr)
            except (AttributeError, KeyError):
                # Filter doesn't apply to this table
                continue

        if applicable_filters:
            query = select(*primary_selection) if isinstance(primary_selection, list) else select(primary_selection)
            if len(applicable_filters) == 1:
                return query.where(applicable_filters[0])
            else:
                return query.where(or_(*applicable_filters))

        # Case 2: Table is indirectly related, use the relationship config
        elif table.name in self.config._relationship_dict:
            path_strings = self.config._relationship_dict[table.name]
            logger.debug(f"[Tenant Deleter] [Collect] '{table.name}' using relationship paths: {path_strings}")

            # Collect queries for all paths (OR logic)
            path_queries = []
            for path_string in path_strings:
                try:
                    parsed_path = _parse_join_path(path_string)
                except ValueError as e:
                    logger.error(f"Could not parse relationship path for '{table.name}': {e}")
                    continue

                if parsed_path['start_table'] != table.name:
                    logger.error(f"Mismatched start table for {table.name} in path '{path_string}'")
                    continue

                if isinstance(primary_selection, list):
                    subquery = select(*primary_selection)
                else:
                    subquery = select(primary_selection)
                join_string = ''
                for i, step in enumerate(parsed_path['steps']):
                    from_tbl = self.metadata.tables[step['from_table']]
                    to_tbl = self.metadata.tables[step['to_table']]
                    subquery = subquery.join(to_tbl, from_tbl.c[step['from_key']] == to_tbl.c[step['to_key']])
                    join_string += f"{step['from_table']}.{step['from_key']}={step['to_table']}.{step['to_key']} "
                    if i < len(parsed_path['steps']) - 1:
                        join_string += '-> '
                logger.info(f"[Tenant Deleter] [Collect] '{table.name}' path:  " + join_string)

                final_table = self.metadata.tables[parsed_path['final_table']]

                # Apply tenant filters to final table
                final_applicable_filters = []
                for tenant_filter in self.config.tenant_filters:
                    try:
                        filter_expr = tenant_filter(final_table)
                        final_applicable_filters.append(filter_expr)
                    except (AttributeError, KeyError):
                        continue

                if final_applicable_filters:
                    if len(final_applicable_filters) == 1:
                        path_queries.append(subquery.where(final_applicable_filters[0]))
                    else:
                        path_queries.append(subquery.where(or_(*final_applicable_filters)))
                else:
                    logger.error(f"Final table '{parsed_path['final_table']}' in path '{path_string}' cannot be filtered by any tenant filters!")  # noqa

            # If we have multiple valid path queries, combine them with UNION
            print(f"[Tenant Deleter] [Collect] '{table.name}' found {len(path_queries)} valid relationship paths")
            if len(path_queries) == 1:
                return path_queries[0]
            elif len(path_queries) > 1:
                # Use UNION to combine all path queries (OR logic)
                return path_queries[0].union(*path_queries[1:])
            else:
                logger.error(f"No valid relationship paths found for table '{table.name}'")
                return None
        return None

    def _collect_pks_to_delete(self):
        """
        PHASE 1: Iterate through all tables and collect the PKs of rows to be deleted.
        The order of table iteration does not matter here.
        """
        logger.info('[Tenant Deleter] Phase 1: Collecting PKs to delete.')
        all_tables = self._build_deletion_order()
        for table in all_tables:
            if table.name in self.excluded_tables:
                logger.info(f"[Tenant Deleter] [Collect] '{table.name}' skipping explicitly excluded table")
                continue

            pk_query = self._build_pk_collection_query(table)
            if pk_query is None:
                raise ValueError(f'Table "{table.name}" found in metadata, but cannot find indirect relationship or applicable tenant filter')  # noqa

            primary_key_columns = list(table.primary_key.columns)
            if len(primary_key_columns) == 1:
                primary_selection = primary_key_columns[0]
            else:
                # For composite keys, select all primary key columns as tuple
                primary_selection = primary_key_columns
            try:
                # Handle composite primary keys in result processing
                if isinstance(primary_selection, list):
                    # For composite keys, we get tuples
                    pks = [tuple(row) for row in self.session.execute(pk_query).all()]
                else:
                    # For single primary key, we get scalars
                    pks = self.session.execute(pk_query).scalars().all()
            except Exception as e:
                logger.error(f"[Tenant Deleter] [Collect] '{table.name}' SQL Execute error: {e}")
                raise
            logger.debug(f"[Tenant Deleter] [Collect] '{table.name}' PK query: {pk_query}")
            if pks:
                logger.info(f"[Tenant Deleter] [Collect] '{table.name}' Found {len(pks)} PKs to delete ")
                self.pks_to_delete[table.name].update(pks)
            else:
                self.pks_to_delete[table.name] = set()
                logger.info(f"[Tenant Deleter] [Collect] '{table.name}' Found 0 PKs to delete")
        logger.info('[Tenant Deleter] Finished Phase 1. PK collection complete.')

    def _execute_deletions(self):
        """
        PHASE 2: Delete the collected PKs in the correct, FK-safe order without orphan issue
        """
        logger.info('[Tenant Deleter] Phase 2: Executing deletions.')
        deletion_order = self._build_deletion_order()

        for table in deletion_order:
            if table.name in self.pks_to_delete:
                pks = list(self.pks_to_delete[table.name])
                if not pks or len(pks) == 0:
                    logger.info(f"[Tenant Deleter] [Execute] Deleting 0 rows from '{table.name}'")
                    continue

                logger.info(f"[Tenant Deleter] [Execute] Deleting {len(pks)} rows from '{table.name}'")
                primary_key_columns = list(table.primary_key.columns)


                # avoid sending a massive IN clause to the DB
                batch_size = self.config.batch_size or 500
                for i in range(0, len(pks), batch_size):
                    batch = pks[i:i + batch_size]
                    # Recreate condition for each batch
                    if len(primary_key_columns) == 1:
                        batch_condition = primary_key_columns[0].in_(batch)
                    else:
                        batch_condition = tuple_(*primary_key_columns).in_(batch)
                    delete_query = table.delete().where(batch_condition)
                    self.session.execute(delete_query)

        logger.info('[Tenant Deleter] Finished Phase 2. Deletions complete.')

    def delete(self, session: Session, dry_run: bool = False, commit: bool = False):
        """
        Delete tenant data using the configured settings.

        Args:
            session: SQLAlchemy session
            dry_run: If True, only report what would be deleted
            commit: If True, commit the transaction
        """
        self.session = session
        start_ms = perf_counter()
        logger.info(
            f'[Tenant Deleter] Starting tenant deletion. '
            f'Dry Run: {dry_run}'
        )

        try:
            # Phase 1: collect the PKs to delete
            self._collect_pks_to_delete()

            # If it's a dry run, report and exit before modifying the DB
            if dry_run:
                logger.info('--- DRY RUN REPORT ---')
                if not self.pks_to_delete:
                    logger.info('No data found for deletion.')
                else:
                    report = {
                        table: f'{len(pks)} rows'
                        for table, pks in self.pks_to_delete.items()
                    }
                    logger.info('The following rows WOULD be deleted:')
                    pprint.pprint(report)
                logger.info('--- END DRY RUN REPORT ---')
                return

            # Phase 2: execute the deletions
            self._execute_deletions()

            if commit:
                logger.info('[Tenant Deleter] Committing transaction.')
                session.commit()
            else:
                session.flush()

            end_ms = perf_counter()
            logger.info(f'[Tenant Deleter] Completed successfully in {end_ms - start_ms:.2f} seconds')

        except Exception as e:
            session.rollback()
            logger.error(f'Error during tenant data deletion: {str(e)}')
            traceback.print_exc()
            raise
