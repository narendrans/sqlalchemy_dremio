## Release 1.2.1

- Single ticks in parametrized statements are escaped
- Table name won't be fixed automatically if no schema is passed. The assumption is that the fully qualified path is properly escaped using quotes. Automated fixed paths lead to issues if a proper path is passed and it will break.

## Release 1.2.0

- Replace parameterized statements by fully compiled, because Dremio does not support parameterized statements.
- Bugfix in get_columns, when Schema is None, the schema "None" was prefixed
- Alias `dialect` for `DremioDialect_pyodbc` created
- Expose supported data types to pyodbc level, so that it works with "Great Expectations"
- Introduced `?filter_schema_names=Space1.Folder1,Space2.Folder2` parameter to filter schemas. If the parameter is not set, all schemas will be returned
- Added has_table implementation for dialect, which seems to be required by latest Apache Superset
