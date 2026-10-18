import type { Migration } from '../migrate.js';
export const elasticCoordinatorSchema: Migration = { name: '119_elastic_coordinator.sql', sql: `
CREATE TABLE elastic_target_projection (host TEXT NOT NULL, session TEXT NOT NULL, worker_id TEXT NOT NULL, revision TEXT NOT NULL, projection TEXT NOT NULL, PRIMARY KEY(host,session));
CREATE TABLE elastic_intake_reservations (qitem_id TEXT PRIMARY KEY, host TEXT NOT NULL, session TEXT NOT NULL, revision TEXT NOT NULL, binding TEXT NOT NULL);
CREATE TABLE elastic_idle_windows (worker_id TEXT PRIMARY KEY, identity TEXT NOT NULL, custody TEXT NOT NULL, owner TEXT NOT NULL, first_at INTEGER NOT NULL, last_at INTEGER NOT NULL);
CREATE TABLE elastic_consumer_progress (consumer TEXT PRIMARY KEY, cursor INTEGER NOT NULL DEFAULT 0);
CREATE TABLE elastic_forward_attempts (intent_id TEXT PRIMARY KEY, attempted INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0, reason TEXT);
CREATE TABLE elastic_native_owners (scope TEXT PRIMARY KEY, owner TEXT NOT NULL, identity TEXT NOT NULL);
` };
