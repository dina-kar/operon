-- Runs once at first start of each Postgres container (docker-entrypoint-initdb.d).
-- The extension goes into template1 so every database created later (PgDog's setup creates
-- several) has it, and into postgres for the reference connection.
\connect template1
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
\connect postgres
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
