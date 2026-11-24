CREATE ROLE fixture_reader NOLOGIN;
CREATE ROLE fixture_gateway LOGIN NOINHERIT PASSWORD 'opspilot-fixture-only';
GRANT fixture_reader TO fixture_gateway;

CREATE SCHEMA demo;
CREATE TABLE demo.items (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL
);
INSERT INTO demo.items (id, label) VALUES (1, 'synthetic readiness fixture');

GRANT USAGE ON SCHEMA demo TO fixture_reader;
GRANT SELECT ON demo.items TO fixture_reader;
