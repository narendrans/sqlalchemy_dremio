# CLAUDE.md - AI Assistant Guide for sqlalchemy_dremio

This document provides comprehensive guidance for AI assistants working with the sqlalchemy_dremio codebase.

## Project Overview

**sqlalchemy_dremio** is a SQLAlchemy dialect for Dremio, a distributed SQL query engine. This library enables Python applications to connect to Dremio using SQLAlchemy's ORM and core APIs via Apache Arrow Flight interface.

- **Current Version**: 3.0.4
- **License**: Apache Software License
- **Python Support**: 3.7+
- **Primary Interface**: Apache Arrow Flight (ODBC deprecated)
- **Repository**: https://github.com/narendrans/sqlalchemy_dremio

## Repository Structure

```
sqlalchemy_dremio/
├── .github/
│   └── workflows/
│       └── dremio.yml           # CI/CD pipeline
├── sqlalchemy_dremio/           # Main package
│   ├── __init__.py              # Package initialization, version, dialect registration
│   ├── base.py                  # Base dialect classes (shared by Flight and deprecated ODBC)
│   ├── flight.py                # Flight dialect implementation (primary interface)
│   ├── db.py                    # Connection and Cursor classes using PyArrow Flight
│   ├── query.py                 # Query execution logic
│   ├── flight_middleware.py    # Cookie middleware for stateful connections
│   └── exceptions.py            # Custom exception classes
├── test/                        # Test suite
│   ├── conftest.py              # Pytest fixtures and configuration
│   ├── test_dremio.py           # Core integration tests
│   ├── test_additional.py       # Additional unit tests
│   └── test_flight_dialect.py  # Flight dialect-specific tests
├── scripts/
│   └── sample.sql               # Sample SQL for test initialization
├── setup.py                     # Package setup and dependencies
├── pyproject.toml               # Ruff linter configuration
├── setup.cfg                    # Version bumping configuration
├── requirements_dev.txt         # Development dependencies
└── README.md                    # User-facing documentation
```

## Core Architecture

### Dialect Implementation

The project implements a SQLAlchemy dialect with the following key components:

1. **DremioDialect_flight** (`flight.py`): Main dialect class registered as `dremio+flight`
2. **DremioCompiler** (`base.py`): SQL compilation with Dremio-specific quirks
3. **DremioDDLCompiler** (`base.py`): DDL statement compilation
4. **DremioIdentifierPreparer** (`base.py`): Identifier quoting with extensive reserved word list

### Connection Flow

```
SQLAlchemy Engine
    ↓
DremioDialect_flight.create_connect_args()
    ↓ (parses URL, builds connection string)
db.connect()
    ↓
Connection.__init__()
    ↓ (establishes Flight client, handles auth)
FlightClient (PyArrow)
```

### Type Mapping

The dialect maps Dremio SQL types to SQLAlchemy types via `_type_map` dictionaries in:
- `flight.py`: Maps SQL type names from DESCRIBE queries
- `query.py`: Maps PyArrow/pandas dtypes from query results

## Key Technical Constraints

### Dremio Limitations

1. **No Parameterized Queries**: Dremio does not support parameterized SQL
   - `base.py:do_execute()` manually substitutes parameters into SQL strings
   - Security implication: Be careful with user input (SQL injection risk)
   - `db.py:executemany()` raises `NotSupportedError`

2. **Limited DDL Support**: Primary keys, foreign keys, indexes not supported
   - Reflection methods return empty lists for these

3. **No Autocommit Control**: Transactions handled server-side
   - `commit()` and `rollback()` are no-ops

### Connection String Format

Flight connection strings follow this pattern:
```
dremio+flight://[user:password@]host:port[/database][?options]
```

**Authentication Options**:
- Basic auth: `UID` and `PWD` parameters (from URL username/password)
- Token auth: `Token=<value>` query parameter

**TLS Options**:
- `UseEncryption=true|false` (default: true)
- `DisableCertificateVerification=true|false`
- `TrustedCerts=/path/to/cert.pem`

**Workload Management**:
- `routing_queue=<queue_name>`
- `routing_tag=<tag>`
- `routing_engine=<engine_name>`

**Other**:
- `Schema=<schema>` (from database part of URL)
- `quoting=<value>`

All query parameter keys are case-insensitive (converted to lowercase in `flight.py:create_connect_args()`).

## Development Workflow

### Setting Up Development Environment

```bash
# Clone repository
git clone https://github.com/narendrans/sqlalchemy_dremio.git
cd sqlalchemy_dremio

# Install development dependencies
pip install -r requirements_dev.txt

# Install package in editable mode
pip install -e .
```

### Running Tests

**Integration tests require a running Dremio instance**:

```bash
# Start Dremio using Docker
docker run -d -p 9047:9047 -p 31010:31010 -p 32010:32010 --name dremio dremio/dremio-oss:latest

# Set connection URL environment variable
export DREMIO_CONNECTION_URL="dremio+flight://dremio:dremio123@localhost:32010/dremio?UseEncryption=false"

# Run tests
pytest

# Or run specific test files
pytest test/test_dremio.py
pytest test/test_additional.py
pytest test/test_flight_dialect.py
```

**Test fixtures** (`test/conftest.py`):
- `engine`: Session-scoped SQLAlchemy engine
- `init_test_schema`: Automatically runs `scripts/sample.sql` before tests

### Code Quality

**Linting with Ruff** (`pyproject.toml`):
```bash
ruff check .
```

Configuration:
- Line length: 100 characters
- Excludes: `scripts/` directory

### Version Management

Uses `bump-my-version` (configured in `setup.cfg`):

```bash
# Bump patch version (e.g., 3.0.4 -> 3.0.5)
bump-my-version bump patch

# Bump minor version (e.g., 3.0.4 -> 3.1.0)
bump-my-version bump minor

# Bump major version (e.g., 3.0.4 -> 4.0.0)
bump-my-version bump major
```

Version is tracked in:
- `setup.py`: `version='...'`
- `sqlalchemy_dremio/__init__.py`: `__version__ = '...'`

## Code Conventions and Style

### General Principles

1. **Python Style**:
   - 4-space indentation (enforced by `.editorconfig`)
   - LF line endings (except `.bat` files use CRLF)
   - UTF-8 encoding
   - Trim trailing whitespace
   - Insert final newline

2. **Import Style**:
   - Use absolute imports
   - Legacy `from __future__ import` statements present in older files
   - Group imports: stdlib, third-party, local

3. **SQL Identifier Quoting**:
   - Always use double quotes: `"table_name"`
   - Handle schema-qualified names: `"schema"."table"`
   - Nested schemas: `"source"."folder"."table"`
   - Reserved words extensively defined in `base.py`

### SQLAlchemy Patterns

1. **SQLAlchemy 2.0 API**:
   - Use `engine.begin()` context manager for transactions
   - Use `text()` for raw SQL
   - Future-style connection API

2. **Reflection**:
   - Use `@reflection.cache` decorator for cached metadata methods
   - Query `INFORMATION_SCHEMA."TABLES"` for table metadata
   - Use `DESCRIBE` for column information

3. **Type Handling**:
   - Maintain both SQL type name and PyArrow dtype mappings
   - Case-insensitive type lookups (both upper and lower case in maps)

### Error Handling

Custom exceptions in `exceptions.py` follow DB-API 2.0 spec:
- `Error`: Base exception
- `DatabaseError`: Database-related errors
- `NotSupportedError`: For unsupported operations (e.g., `executemany`)
- `InterfaceError`, `OperationalError`, `ProgrammingError`, etc.

Use decorators for common checks:
- `@check_closed`: Verify connection/cursor is open
- `@check_result`: Verify cursor has executed a query

## Testing Conventions

### Test Organization

1. **test_dremio.py**: Core integration tests
   - Connection tests
   - Basic SQL execution
   - Table existence checks
   - Uses helper functions like `_conn()`, `_table_exists()`

2. **test_additional.py**: Unit tests with mocking
   - Dialect configuration tests
   - Cursor behavior tests
   - Uses `monkeypatch` for isolation

3. **test_flight_dialect.py**: Flight-specific functionality
   - Connection string parsing
   - URL option handling

### Test Helpers

```python
def _conn():
    """Get a connection, skip test if DREMIO_CONNECTION_URL not set"""
    engine = conftest.get_engine()
    if engine is None:
        pytest.skip("DREMIO_CONNECTION_URL not set")
    return engine.connect()
```

### Environment Variables

- `DREMIO_CONNECTION_URL`: Required for integration tests
- Example: `dremio+flight://dremio:dremio123@localhost:32010/dremio?UseEncryption=false`

## CI/CD Pipeline

GitHub Actions workflow (`.github/workflows/dremio.yml`):

1. **Triggers**: Push to `master`, pull requests, manual dispatch
2. **Environment**: Ubuntu latest, Python 3.13
3. **Services**: Dremio OSS Docker container
4. **Steps**:
   - Checkout code
   - Set up Python
   - Install dependencies from `requirements_dev.txt`
   - Wait for Dremio to be ready (health check on port 9047)
   - Install package in editable mode
   - Run pytest with connection URL

## Common Tasks for AI Assistants

### Adding a New Feature

1. Determine if it affects:
   - Dialect behavior → modify `flight.py` or `base.py`
   - Connection logic → modify `db.py`
   - Query execution → modify `query.py`
   - Type mapping → update `_type_map` in relevant files

2. Add tests in appropriate test file
3. Update README.md if user-facing
4. Run tests locally before committing

### Fixing a Bug

1. **Reproduce**: Write a failing test first
2. **Locate**: Check error location:
   - Connection errors → `db.py`
   - SQL generation issues → `base.py` compilers
   - Type conversion errors → `query.py` or type maps
   - URL parsing → `flight.py:create_connect_args()`

3. **Fix**: Make minimal changes to fix the issue
4. **Test**: Ensure the test passes and no regressions
5. **Document**: Update RELEASE_NOTES.md if significant

### Updating Dependencies

1. **Core dependencies** (`setup.py`):
   - `SQLAlchemy~=2.0.41`
   - `pyarrow~=20.0.0`

2. **Dev dependencies** (`requirements_dev.txt`):
   - Update version constraints
   - Test thoroughly as these affect CI

3. **Test compatibility**: Run full test suite

### Working with Identifiers

When generating SQL:
```python
# BAD - no quoting
sql = f"SELECT * FROM {table_name}"

# GOOD - use double quotes
sql = f'SELECT * FROM "{table_name}"'

# BETTER - use the preparer
from sqlalchemy.sql import compiler
preparer = DremioIdentifierPreparer(dialect)
quoted = preparer.quote_identifier(table_name)
```

### Handling Connection Strings

Parse with case-insensitive option lookup:
```python
lc_query_dict = {k.lower(): v for k, v in url.query.items()}
if 'useencryption' in lc_query_dict:
    # Process UseEncryption option
```

## Security Considerations

1. **SQL Injection Risk**: Since Dremio doesn't support parameterized queries, the dialect manually injects parameters. Always validate/sanitize user input.

2. **TLS**: Default is encrypted connections (`UseEncryption=true`)

3. **Certificate Verification**: Can be disabled but warn users about security implications

4. **Credentials**: Support both username/password and token authentication

## Dremio-Specific SQL Quirks

1. **Table Paths**: Can be deeply nested (e.g., `source.folder.subfolder.table`)
2. **System Tables**: `sys.version`, `INFORMATION_SCHEMA."TABLES"`
3. **Special Schemas**: `$scratch` for temporary tables
4. **Function Names**: Some SQL functions renamed (e.g., `CHAR_LENGTH` → `LENGTH`)
5. **No Traditional Constraints**: No primary keys, foreign keys, or indexes in metadata

## File-by-File Reference

### `sqlalchemy_dremio/__init__.py`
- Package version declaration
- Dialect registration with SQLAlchemy
- Re-exports `Connection` and `connect`

### `sqlalchemy_dremio/flight.py`
- `DremioDialect_flight`: Main dialect class
- `create_connect_args()`: Parses SQLAlchemy URL to connection string
- Inherits compilers and preparers from `base.py`
- Implements reflection methods: `get_columns`, `get_table_names`, `has_table`, etc.

### `sqlalchemy_dremio/base.py`
- `DremioCompiler`: Customizes SQL compilation (table quoting, function names)
- `DremioDDLCompiler`: DDL statement generation
- `DremioIdentifierPreparer`: Identifier quoting with reserved words
- `DremioDialect`: Base ODBC dialect (deprecated)
- Extensive Dremio reserved word set

### `sqlalchemy_dremio/db.py`
- `Connection`: Manages PyArrow FlightClient
- `Cursor`: Executes queries and fetches results
- Decorators: `@check_closed`, `@check_result`
- Authentication logic (basic auth vs. token)
- TLS configuration
- Header propagation (routing, schema, etc.)

### `sqlalchemy_dremio/query.py`
- `execute()`: Runs query via Flight, returns rows and description
- `run_query()`: Low-level Flight query execution
- PyArrow to pandas conversion
- Type mapping from pandas dtypes

### `sqlalchemy_dremio/flight_middleware.py`
- `CookieMiddleware`: Manages cookies for stateful connections
- Receives `Set-Cookie` headers
- Sends `Cookie` headers on subsequent requests

### `sqlalchemy_dremio/exceptions.py`
- DB-API 2.0 compliant exception hierarchy
- All inherit from `Error` or `Warning`

## Useful Resources

- **Dremio Documentation**: https://docs.dremio.com/
- **SQLAlchemy Documentation**: https://docs.sqlalchemy.org/
- **PyArrow Flight**: https://arrow.apache.org/docs/python/flight.html
- **Dremio Flight Endpoint**: https://docs.dremio.com/software/client-applications/arrow-flight/
- **GitHub Repository**: https://github.com/narendrans/sqlalchemy_dremio

## Common Pitfalls

1. **Forgetting DREMIO_CONNECTION_URL**: Tests will be skipped
2. **Not waiting for Dremio startup**: Docker container takes ~30-60 seconds to initialize
3. **Case sensitivity in options**: URL query parameters are converted to lowercase
4. **Schema quoting**: Multi-level schemas need proper quote handling
5. **Type mapping gaps**: New Dremio types may not be in `_type_map`
6. **Connection pooling**: Uses `SingletonThreadPool` - be aware in multi-threaded apps

## Git Workflow

- **Main branch**: `master`
- **Commit messages**: Descriptive, follow conventional commits style when possible
- **PR requirements**: Tests must pass in CI
- **Version bumps**: Update version before releases using `bump-my-version`

## Questions to Ask Before Making Changes

1. Does this affect the public API? (Update README)
2. Does this require a version bump? (Breaking change? New feature?)
3. Are there integration tests? (May need Dremio running)
4. Does this work with both auth methods? (Basic and Token)
5. Does this work with TLS and non-TLS connections?
6. Are there any Dremio version-specific considerations?
7. Does this maintain SQLAlchemy 2.0 compatibility?

## Contact and Support

- **Issues**: https://github.com/narendrans/sqlalchemy_dremio/issues
- **Author**: Naren (me@narendran.info)
