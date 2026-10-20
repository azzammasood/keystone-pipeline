-- Privacy (CLAUDE.md): published layers hold repository and package facts only. Fails on any
-- column that looks like a person key, login, email or name of a person.
SELECT table_schema, table_name, column_name
FROM information_schema.columns
WHERE
    LOWER(table_schema) IN ('marts', 'metrics')
    AND (
        LOWER(column_name) LIKE '%actor%'
        OR LOWER(column_name) LIKE '%login%'
        OR LOWER(column_name) LIKE '%email%'
        OR LOWER(column_name) LIKE '%author%'
        OR LOWER(column_name) LIKE '%owner%'
    )
