import type { Migration } from '../migrate.js';

export const elasticContinuationSchema: Migration = {
  name: '120_elastic_continuation.sql',
  sql: `
    ALTER TABLE elastic_worker_custody RENAME TO elastic_worker_custody_legacy;
    CREATE TABLE elastic_worker_custody (
      qitem_id TEXT NOT NULL, parent_id TEXT, generation INTEGER NOT NULL,
      source_session TEXT, destination_session TEXT, lineage TEXT,
      PRIMARY KEY(generation,qitem_id)
    );
    INSERT INTO elastic_worker_custody SELECT * FROM elastic_worker_custody_legacy;
    DROP TRIGGER elastic_worker_insert;
    DROP TRIGGER elastic_worker_update;
    DROP TABLE elastic_worker_custody_legacy;
    CREATE TABLE elastic_worker_generations (
      generation INTEGER PRIMARY KEY, purpose TEXT NOT NULL CHECK(purpose IN ('removal','continuation')),
      operation_id TEXT NOT NULL UNIQUE, predecessor INTEGER, withdrawal_id TEXT UNIQUE,
      selector_digest TEXT, created_at INTEGER NOT NULL, retired_at INTEGER,
      disposition_digest TEXT, progress_at INTEGER
    );
    INSERT INTO elastic_worker_generations(generation,purpose,operation_id,created_at)
      SELECT generation,'removal',drain_operation_id,drain_started_at FROM elastic_worker_control WHERE generation>0;
    CREATE TABLE elastic_native_wakes (
      outbox_id TEXT PRIMARY KEY, qitem_id TEXT NOT NULL, generation INTEGER NOT NULL,
      parent_id TEXT, parent_digest TEXT, queue_digest TEXT NOT NULL, outbox_digest TEXT NOT NULL
    );
    CREATE TABLE elastic_continuation_actions (
      action_id TEXT PRIMARY KEY, generation INTEGER NOT NULL, withdrawal_id TEXT NOT NULL,
      qitem_id TEXT NOT NULL, outbox_id TEXT NOT NULL UNIQUE, action_json TEXT NOT NULL,
      stage TEXT NOT NULL CHECK(stage IN ('prepared','transport-issued','spawn-issued','input-issued','proved')),
      runner_json TEXT, receipt_json TEXT, created_at INTEGER NOT NULL, proved_at INTEGER
    );
    CREATE TRIGGER elastic_worker_insert BEFORE INSERT ON queue_items
    WHEN EXISTS(SELECT 1 FROM elastic_worker_control WHERE phase != 'ready')
    BEGIN
      SELECT CASE WHEN EXISTS(SELECT 1 FROM queue_items WHERE qitem_id=NEW.qitem_id) THEN RAISE(ABORT, 'elastic_worker_fenced') END;
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM elastic_worker_control c JOIN elastic_worker_custody p ON p.generation=c.generation
        WHERE c.phase='draining' AND p.qitem_id=NEW.qitem_id AND p.source_session=NEW.source_session AND p.destination_session=NEW.destination_session
          AND (p.lineage='human-response' OR (p.lineage='handoff' AND p.parent_id=NEW.handed_off_from))
      ) THEN RAISE(ABORT, 'elastic_worker_fenced') END;
    END;
    CREATE TRIGGER elastic_worker_update BEFORE UPDATE ON queue_items
    WHEN NEW.state IN ('pending','in-progress','blocked')
      AND EXISTS(SELECT 1 FROM elastic_worker_control WHERE phase != 'ready')
    BEGIN
      SELECT CASE WHEN NOT EXISTS (
        SELECT 1 FROM elastic_worker_control c JOIN elastic_worker_custody p ON p.generation=c.generation
        WHERE c.phase='draining' AND p.qitem_id=OLD.qitem_id AND NEW.qitem_id=OLD.qitem_id
          AND OLD.state IN ('pending','in-progress','blocked')
          AND NEW.destination_session=OLD.destination_session AND NEW.source_session=OLD.source_session
      ) THEN RAISE(ABORT, 'elastic_worker_fenced') END;
    END;
  `,
};
