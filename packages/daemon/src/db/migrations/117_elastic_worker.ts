import type { Migration } from '../migrate.js';

export const elasticWorkerSchema: Migration = {
  name: '117_elastic_worker.sql',
  sql: `
    CREATE TABLE elastic_worker_control (
      id INTEGER PRIMARY KEY CHECK(id=1), worker_id TEXT NOT NULL, vm_id TEXT NOT NULL,
      manifest_digest TEXT NOT NULL, phase TEXT NOT NULL CHECK(phase IN ('ready','draining','sealed')),
      generation INTEGER NOT NULL DEFAULT 0, drain_operation_id TEXT, drain_started_at INTEGER
    );
    CREATE TABLE elastic_worker_custody (
      qitem_id TEXT PRIMARY KEY, parent_id TEXT, generation INTEGER NOT NULL,
      source_session TEXT, destination_session TEXT, lineage TEXT
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
    CREATE TRIGGER elastic_worker_delete BEFORE DELETE ON queue_items
    WHEN OLD.state IN ('pending','in-progress','blocked')
      AND EXISTS(SELECT 1 FROM elastic_worker_control WHERE phase != 'ready')
    BEGIN SELECT RAISE(ABORT, 'elastic_worker_fenced'); END;
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
