-- A small workload through PgDog's two-shard container (compose.pg.yml, profile baseline), written for
-- Loams: DDL broadcast, single-shard and cross-shard DML, a two-phase-commit transaction, aggregates, COPY.
-- Run with: psql -h 127.0.0.1 -p 6432 -U pgdog -d inv -f pg-baseline.sql
DROP TABLE IF EXISTS events;
CREATE TABLE events (id bigint PRIMARY KEY, kind text NOT NULL, amount numeric(12,2) DEFAULT 0, at timestamptz DEFAULT now());
CREATE INDEX events_kind_idx ON events (kind);
INSERT INTO events (id, kind, amount) VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'a', 3.5), (4, 'c', 4.5);
INSERT INTO events (id, kind) VALUES (5, 'a');
SELECT * FROM events WHERE id = 3;
SELECT kind, count(*), sum(amount) FROM events GROUP BY kind ORDER BY kind;
SELECT count(*) FROM events;
UPDATE events SET amount = amount + 1 WHERE id = 2;
BEGIN;
INSERT INTO events (id, kind) VALUES (100, 'tx'), (101, 'tx'), (102, 'tx'), (103, 'tx');
UPDATE events SET kind = 'tx2' WHERE kind = 'tx';
COMMIT;
BEGIN;
DELETE FROM events WHERE kind = 'tx2';
ROLLBACK;
SELECT * FROM events ORDER BY id LIMIT 3;
PREPARE by_id (bigint) AS SELECT * FROM events WHERE id = $1;
EXECUTE by_id (1);
DEALLOCATE by_id;
COPY events (id, kind) FROM STDIN;
200	copy
201	copy
202	copy
\.
COPY (SELECT id, kind FROM events WHERE kind = 'copy') TO STDOUT;
SHOW server_version;
DELETE FROM events WHERE id >= 200;
TRUNCATE events;
