import { sql } from 'kysely';
import type { KyselyDb } from './kysely.js';

/**
 * Run migrations for the events table.
 * Creates the table and all necessary indexes/triggers for the outbox pattern.
 */
export async function runEventsMigration(db: KyselyDb): Promise<void> {
  // Create base events table
  await db.schema
    .createTable('events')
    .ifNotExists()
    .addColumn('event_id', 'text', (col) => col.primaryKey())
    .addColumn('event_name', 'text', (col) => col.notNull())
    .addColumn('stream_id', 'text', (col) => col.notNull())
    .addColumn('event', 'jsonb', (col) => col.notNull())
    .addColumn('timestamp', 'timestamptz', (col) => col.notNull())
    .addColumn('status', 'text', (col) => col.notNull())
    .addColumn('type', 'text', (col) => col.notNull())
    .addColumn('meta', 'jsonb')
    .addColumn('retry_count', 'integer', (col) => col.defaultTo(0))
    .addColumn('locked_at', 'timestamptz')
    .addColumn('locked_by', 'text')
    .addColumn('next_retry_at', 'timestamptz', (col) => col.defaultTo(sql`NOW()`))
    .execute();

  // Create polling index (partial index - requires raw SQL)
  await sql`
    CREATE INDEX IF NOT EXISTS idx_events_pending_poll
    ON events (status, type, next_retry_at, timestamp)
    WHERE status IN ('CREATED', 'FAILED')
  `.execute(db);

  // Create stale lock cleanup index (partial index - requires raw SQL)
  await sql`
    CREATE INDEX IF NOT EXISTS idx_events_stale_locks
    ON events (locked_at)
    WHERE locked_at IS NOT NULL AND status = 'PROCESSING'
  `.execute(db);

  // Create LISTEN/NOTIFY function for executeSync (requires raw SQL)
  await sql`
    CREATE OR REPLACE FUNCTION notify_event_status_change()
    RETURNS TRIGGER AS $$
    BEGIN
      IF NEW.status IN ('PROCESSED', 'FAILED', 'ABORTED') THEN
        PERFORM pg_notify(
          'cqrs_event_status',
          json_build_object(
            'event_id', NEW.event_id,
            'status', NEW.status
          )::text
        );
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
  `.execute(db);

  // Drop and recreate trigger to ensure correct definition
  await sql`DROP TRIGGER IF EXISTS event_status_notify ON events`.execute(db);

  await sql`
    CREATE TRIGGER event_status_notify
    AFTER UPDATE OF status ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_event_status_change()
  `.execute(db);
}

/**
 * Run migrations for the scheduled_events table.
 */
export async function runScheduledEventsMigration(db: KyselyDb): Promise<void> {
  await db.schema
    .createTable('scheduled_events')
    .ifNotExists()
    .addColumn('scheduled_event_id', 'text', (col) => col.primaryKey())
    .addColumn('execute_at', 'timestamptz', (col) => col.notNull())
    .addColumn('event', 'jsonb', (col) => col.notNull())
    .addColumn('status', 'text', (col) => col.notNull())
    .execute();
}

/**
 * Run all CQRS migrations.
 */
export async function runAllMigrations(db: KyselyDb): Promise<void> {
  await runEventsMigration(db);
  await runScheduledEventsMigration(db);
}
