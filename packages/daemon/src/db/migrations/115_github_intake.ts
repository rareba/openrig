import type { Migration } from "../migrate.js";

export const githubIntakeSchema: Migration = {
  name: "115_github_intake.sql",
  sql: `
    CREATE TABLE github_intake_config (id INTEGER PRIMARY KEY CHECK (id = 1), config TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0, error TEXT, last_poll TEXT);
    CREATE TABLE github_intake_issues (
      repository_id TEXT NOT NULL, issue_number INTEGER NOT NULL, issue_json TEXT NOT NULL,
      eligibility TEXT NOT NULL DEFAULT 'triage', priority TEXT NOT NULL DEFAULT 'routine', first_seen TEXT NOT NULL, approved_revision TEXT, capability TEXT, reason TEXT,
      in_scope INTEGER NOT NULL DEFAULT 1, generation INTEGER NOT NULL DEFAULT 0,
      qitem_id TEXT, target_host TEXT, target_session TEXT, target_machine TEXT, current_host TEXT, current_session TEXT, stage TEXT,
      delivery TEXT, pickup TEXT, error TEXT,
      PRIMARY KEY (repository_id, issue_number)
    );
  `,
};
