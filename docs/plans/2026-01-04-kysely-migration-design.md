# Kysely Migration Design

**Date:** 2026-01-04
**Status:** Approved
**Scope:** Replace manual SQL strings with Kysely query builder

## Overview

Refactor the CQRS library to use Kysely instead of manual SQL string construction. This provides full type safety, automatic case conversion, and cleaner query building while maintaining PGLite compatibility for testing.

## Decisions

- **Approach:** Full Kysely conversion with typed schema
- **Migrations:** Keep runtime migrations using Kysely schema builder
- **Testing:** Use `kysely-pglite-dialect` for in-memory PostgreSQL tests
- **Isolation Levels:** Simplify enum to match Kysely's expected values directly

## Database Schema Types

```typescript
// src/infrastructure/db/schema.ts

import type { Generated, ColumnType } from 'kysely';

interface EventsTable {
  event_id: string;
  event_name: string;
  stream_id: string;
  event: ColumnType<unknown, string, string>;
  timestamp: ColumnType<Date, Date | string, Date | string>;
  status: 'CREATED' | 'PROCESSING' | 'FAILED' | 'PROCESSED' | 'ABORTED';
  type: 'COMMAND' | 'QUERY' | 'EVENT';
  meta: ColumnType<unknown | null, string | null, string | null>;
  retry_count: Generated<number>;
  locked_at: ColumnType<Date | null, Date | string | null, Date | string | null>;
  locked_by: string | null;
  next_retry_at: ColumnType<Date | null, Date | string | null, Date | string | null>;
}

interface ScheduledEventsTable {
  scheduled_event_id: string;
  execute_at: ColumnType<Date, Date | string, Date | string>;
  event: ColumnType<unknown, string, string>;
  status: 'CREATED' | 'FAILED' | 'PROCESSED' | 'ABORTED';
}

export interface Database {
  events: EventsTable;
  scheduled_events: ScheduledEventsTable;
}
```

## Kysely Instance Factory

```typescript
// src/infrastructure/db/kysely.ts

import { Kysely, PostgresDialect, CamelCasePlugin } from 'kysely';
import type { Pool } from 'pg';
import type { Database } from './schema.js';

export type KyselyDb = Kysely<Database>;

export function createKyselyFromPool(pool: Pool): KyselyDb {
  return new Kysely<Database>({
    dialect: new PostgresDialect({ pool }),
    plugins: [new CamelCasePlugin()],
  });
}
```

## Runtime Migrations

- Use Kysely schema builder for table creation (`createTable().ifNotExists()`)
- Use `sql` template tag for PostgreSQL-specific features:
  - Partial indexes
  - Triggers and functions
  - `jsonb_set()` operations

## Key Refactoring Patterns

### Insert with Upsert
```typescript
await db
  .insertInto('events')
  .values(data)
  .onConflict(oc => oc.column('eventId').doUpdateSet({ ... }))
  .execute();
```

### Dynamic Where Clauses
```typescript
let qb = db.selectFrom('events').selectAll();
if (query.eventId) qb = qb.where('eventId', '=', query.eventId);
if (query.status) qb = qb.where('status', '=', query.status);
return await qb.execute();
```

### FOR UPDATE SKIP LOCKED
```typescript
const subquery = trx
  .selectFrom('events')
  .select('eventId')
  .where('status', '=', status)
  .forUpdate()
  .skipLocked();

await trx
  .updateTable('events')
  .set({ status: 'PROCESSING' })
  .where('eventId', 'in', subquery)
  .returningAll()
  .execute();
```

### Transactions
```typescript
await db.transaction()
  .setIsolationLevel(isolationLevel)
  .execute(async (trx) => {
    // All operations use trx
  });
```

## Files Changed

| File | Change | Description |
|------|--------|-------------|
| `src/infrastructure/db/schema.ts` | New | Database type definitions |
| `src/infrastructure/db/kysely.ts` | New | Kysely instance factory |
| `src/infrastructure/db/migrations.ts` | New | Runtime migration functions |
| `src/infrastructure/db/adapter.ts` | Update | Simplify IsolationLevel enum |
| `src/infrastructure/PersistentEventStore.ts` | Refactor | Use Kysely queries |
| `src/infrastructure/PersistentEventScheduler.ts` | Refactor | Use Kysely queries |
| `src/infrastructure/PollingWorker.ts` | Refactor | Use Kysely queries |
| `src/decorators/UowDecorator.ts` | Refactor | Use Kysely transactions |
| `src/test/pgliteAdapter.ts` | Refactor | Use kysely-pglite-dialect |
| `src/types.ts` | Update | ITransactionalScope type |

## Dependencies

**Add:**
- `kysely: ^0.27.0`

**Add (devDependencies):**
- `kysely-pglite-dialect: ^0.7.0`

## Breaking Changes

1. `IsolationLevel` enum values change from uppercase to lowercase
2. `ITransactionalScope` changes from `PoolClient` to `Transaction<Database>`
3. `IPostgresSettings` requires `db: KyselyDb` instead of just `pool`

## Testing Strategy

- Use `kysely-pglite-dialect` with `@electric-sql/pglite` for unit tests
- Same `CamelCasePlugin` ensures consistent behavior
- Mock pool for LISTEN/NOTIFY features
