import type { Migration } from "../migrate.js";

export const elasticFleetSchema: Migration = {
  name: "116_elastic_fleet.sql",
  sql: `
    CREATE TABLE elastic_fleet_config (
      id INTEGER PRIMARY KEY CHECK (id = 1), config TEXT, revision INTEGER NOT NULL DEFAULT 0,
      paused INTEGER NOT NULL DEFAULT 0, history TEXT NOT NULL DEFAULT '{"lastObservedAt":null,"demandSamples":0}',
      last_plan TEXT, error TEXT, reconcile_owner TEXT, updated_at INTEGER NOT NULL DEFAULT 0
    );
    INSERT INTO elastic_fleet_config (id) VALUES (1);
    CREATE TABLE elastic_fleet_operations (
      operation_id TEXT PRIMARY KEY, type TEXT NOT NULL, state TEXT NOT NULL,
      worker_id TEXT, count INTEGER NOT NULL, target_capacity INTEGER NOT NULL, revision INTEGER NOT NULL,
      snapshot TEXT NOT NULL, configuration TEXT NOT NULL, error TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
    CREATE UNIQUE INDEX elastic_fleet_one_pending ON elastic_fleet_operations (state) WHERE state = 'pending';
    CREATE TABLE elastic_fleet_drains (
      worker_id TEXT PRIMARY KEY, pool_id TEXT NOT NULL, state TEXT NOT NULL,
      begin_operation_id TEXT, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
    );
  `,
};
