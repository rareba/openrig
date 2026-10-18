import type { Migration } from '../migrate.js';

export const elasticHumanProxySchema: Migration = {
  name: '118_elastic_human_proxy.sql',
  sql: `
    -- Internal capability is visible only inside its owning SQLite write transaction.
    -- Every public/legacy writer sees 0, including connections without a QueueRepository.
    CREATE TABLE elastic_human_write_scope (id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL CHECK(enabled IN (0,1)));
    INSERT INTO elastic_human_write_scope VALUES(1,0);
    CREATE TABLE elastic_human_proxies (
      mapping_id TEXT PRIMARY KEY, proxy_qitem_id TEXT NOT NULL UNIQUE,
      worker_id TEXT NOT NULL, origin_qitem_id TEXT NOT NULL, sequence INTEGER NOT NULL,
      mapping TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('open','forward-pending','awaiting-origin','settled','superseded','conflict')),
      intent_id TEXT UNIQUE, receipt_qitem_id TEXT UNIQUE, origin_receipt TEXT, origin_readback TEXT,
      UNIQUE(worker_id,origin_qitem_id,sequence)
    );
    CREATE UNIQUE INDEX elastic_human_one_active_origin ON elastic_human_proxies(worker_id,origin_qitem_id)
      WHERE state IN ('open','forward-pending','awaiting-origin','conflict');
    CREATE TRIGGER elastic_human_transport_insert BEFORE INSERT ON queue_items
    WHEN EXISTS(SELECT 1 FROM elastic_human_proxies WHERE proxy_qitem_id=NEW.qitem_id OR receipt_qitem_id=NEW.qitem_id)
      AND (SELECT enabled FROM elastic_human_write_scope WHERE id=1)=0
    BEGIN SELECT RAISE(ABORT,'elastic_human_transport_guard'); END;
    CREATE TRIGGER elastic_human_transport_update BEFORE UPDATE ON queue_items
    WHEN EXISTS(SELECT 1 FROM elastic_human_proxies WHERE proxy_qitem_id=OLD.qitem_id OR receipt_qitem_id=OLD.qitem_id)
      AND (SELECT enabled FROM elastic_human_write_scope WHERE id=1)=0
    BEGIN SELECT RAISE(ABORT,'elastic_human_transport_guard'); END;
    CREATE TRIGGER elastic_human_transport_delete BEFORE DELETE ON queue_items
    WHEN EXISTS(SELECT 1 FROM elastic_human_proxies WHERE proxy_qitem_id=OLD.qitem_id OR receipt_qitem_id=OLD.qitem_id)
    BEGIN SELECT RAISE(ABORT,'elastic_human_transport_guard'); END;
    CREATE TRIGGER elastic_human_no_dependents_insert BEFORE INSERT ON queue_items
    WHEN EXISTS(SELECT 1 FROM elastic_human_proxies WHERE proxy_qitem_id=NEW.blocked_on OR receipt_qitem_id=NEW.blocked_on)
    BEGIN SELECT RAISE(ABORT,'elastic_human_transport_dependency'); END;
    CREATE TRIGGER elastic_human_no_dependents_update BEFORE UPDATE OF blocked_on ON queue_items
    WHEN EXISTS(SELECT 1 FROM elastic_human_proxies WHERE proxy_qitem_id=NEW.blocked_on OR receipt_qitem_id=NEW.blocked_on)
    BEGIN SELECT RAISE(ABORT,'elastic_human_transport_dependency'); END;
  `,
};
