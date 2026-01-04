// import { PGlite } from "@electric-sql/pglite";
// import type { QueryResult, QueryResultRow } from "pg";
// import type { DbNotification } from "../infrastructure/db/adapter.js";
// import { IsolationLevel } from "../infrastructure/db/adapter.js";

// /**
//  * Check if a SQL string contains multiple statements.
//  */
// function hasMultipleStatements(sql: string): boolean {
//   const cleaned = sql
//     .replace(/'[^']*'/g, "''")
//     .replace(/--[^\n]*/g, "")
//     .replace(/\/\*[\s\S]*?\*\//g, "");

//   const statements = cleaned
//     .split(";")
//     .map((s) => s.trim())
//     .filter((s) => s.length > 0);

//   return statements.length > 1;
// }

// /**
//  * Convert PGlite result to pg.QueryResult format.
//  */
// function toPgResult<R extends QueryResultRow>(result: {
//   rows: R[];
//   affectedRows?: number;
//   fields?: Array<{ name: string; dataTypeID?: number | string }>;
// }): QueryResult<R> {
//   return {
//     rows: result.rows,
//     rowCount: result.affectedRows ?? result.rows.length,
//     command: "",
//     oid: 0,
//     fields: (result.fields || []).map((f) => ({
//       name: f.name,
//       tableID: 0,
//       columnID: 0,
//       dataTypeID: parseInt(f.dataTypeID?.toString() || "0", 10),
//       dataTypeSize: 0,
//       dataTypeModifier: 0,
//       format: "text" as const,
//     })),
//   };
// }

// /**
//  * PGlite-based implementation of IDbClient.
//  */
// export class PGliteClient implements IDbClient {
//   private notificationListeners: Array<(msg: DbNotification) => void> = [];
//   private activeListens: Map<string, () => Promise<void>> = new Map();

//   constructor(private db: PGlite) {}

//   async query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>> {
//     const trimmedText = text.trim().toUpperCase();

//     // Handle LISTEN command
//     const listenMatch = trimmedText.match(/^LISTEN\s+(\w+)/i);
//     if (listenMatch) {
//       const channel = listenMatch[1].toLowerCase();
//       await this.setupListen(channel);
//       return toPgResult({ rows: [] as R[], affectedRows: 0 });
//     }

//     // Handle UNLISTEN command
//     const unlistenMatch = trimmedText.match(/^UNLISTEN\s+(\w+)/i);
//     if (unlistenMatch) {
//       const channel = unlistenMatch[1].toLowerCase();
//       await this.teardownListen(channel);
//       return toPgResult({ rows: [] as R[], affectedRows: 0 });
//     }

//     // If no parameters and multiple statements, use exec()
//     if ((!values || values.length === 0) && hasMultipleStatements(text)) {
//       const results = await this.db.exec(text);
//       const lastResult = results[results.length - 1] || { rows: [], affectedRows: 0 };
//       return toPgResult(lastResult as { rows: R[]; affectedRows?: number });
//     }

//     const result = await this.db.query<R>(text, values as any[]);
//     return toPgResult(result);
//   }

//   private async setupListen(channel: string): Promise<void> {
//     if (this.activeListens.has(channel)) {
//       return;
//     }

//     const unsub = await this.db.listen(channel, (payload) => {
//       const notification: DbNotification = {
//         channel,
//         payload,
//         processId: 0,
//       };
//       this.notificationListeners.forEach((listener) => listener(notification));
//     });

//     this.activeListens.set(channel, unsub);
//   }

//   private async teardownListen(channel: string): Promise<void> {
//     const unsub = this.activeListens.get(channel);
//     if (unsub) {
//       await unsub();
//       this.activeListens.delete(channel);
//     }
//   }

//   on(event: "notification", callback: (msg: DbNotification) => void): this {
//     if (event === "notification") {
//       this.notificationListeners.push(callback);
//     }
//     return this;
//   }

//   off(event: "notification", callback: (msg: DbNotification) => void): this {
//     if (event === "notification") {
//       const idx = this.notificationListeners.indexOf(callback);
//       if (idx >= 0) {
//         this.notificationListeners.splice(idx, 1);
//       }
//     }
//     return this;
//   }

//   release(): void {
//     this.activeListens.forEach(async (unsub) => {
//       try {
//         await unsub();
//       } catch {
//         // Ignore errors during cleanup
//       }
//     });
//     this.activeListens.clear();
//     this.notificationListeners = [];
//   }
// }

// /**
//  * Simple mutex for serializing database operations.
//  */
// class Mutex {
//   private queue: Array<() => void> = [];
//   private locked = false;

//   async acquire(): Promise<void> {
//     return new Promise((resolve) => {
//       if (!this.locked) {
//         this.locked = true;
//         resolve();
//       } else {
//         this.queue.push(resolve);
//       }
//     });
//   }

//   release(): void {
//     if (this.queue.length > 0) {
//       const next = this.queue.shift()!;
//       next();
//     } else {
//       this.locked = false;
//     }
//   }
// }

// /**
//  * PGlite-based implementation of IDbAdapter.
//  *
//  * Note: PGlite is single-threaded, so we use a mutex to serialize
//  * transaction access and prevent concurrent transaction conflicts.
//  */
// export class PGliteAdapter implements IDbAdapter {
//   private db: PGlite | null = null;
//   private initPromise: Promise<void> | null = null;
//   private txMutex = new Mutex();
//   private inTransaction = false;

//   constructor() {
//     this.initPromise = this.init();
//   }

//   private async init(): Promise<void> {
//     this.db = await PGlite.create();
//   }

//   private async ensureReady(): Promise<PGlite> {
//     await this.initPromise;
//     if (!this.db) {
//       throw new Error("PGlite database not initialized");
//     }
//     return this.db;
//   }

//   async query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>> {
//     const db = await this.ensureReady();

//     // If no parameters and multiple statements, use exec()
//     if ((!values || values.length === 0) && hasMultipleStatements(text)) {
//       const results = await db.exec(text);
//       const lastResult = results[results.length - 1] || { rows: [], affectedRows: 0 };
//       return toPgResult(lastResult as { rows: R[]; affectedRows?: number });
//     }

//     const result = await db.query<R>(text, values as any[]);
//     return toPgResult(result);
//   }

//   async connect(): Promise<IDbClient> {
//     const db = await this.ensureReady();
//     // Return a client that serializes BEGIN/COMMIT access via the mutex
//     return new PGliteMutexClient(db, this.txMutex, () => this.inTransaction, (val) => { this.inTransaction = val; });
//   }

//   async transaction<T>(
//     isolationLevel: IsolationLevel,
//     callback: (client: ITransactionClient) => Promise<T>
//   ): Promise<T> {
//     const db = await this.ensureReady();

//     // Acquire mutex to ensure no concurrent transactions
//     await this.txMutex.acquire();
//     this.inTransaction = true;

//     const pgliteClient = new PGliteClient(db) as ITransactionClient;
//     (pgliteClient as any)._inTransaction = true;

//     try {
//       await db.query(`BEGIN ISOLATION LEVEL ${isolationLevel}`);
//       const result = await callback(pgliteClient);
//       await db.query("COMMIT");
//       return result;
//     } catch (error) {
//       await db.query("ROLLBACK");
//       throw error;
//     } finally {
//       this.inTransaction = false;
//       this.txMutex.release();
//     }
//   }

//   async end(): Promise<void> {
//     if (this.db) {
//       await this.db.close();
//       this.db = null;
//     }
//   }
// }

// /**
//  * PGlite client that serializes transaction commands via mutex.
//  */
// class PGliteMutexClient implements IDbClient {
//   private notificationListeners: Array<(msg: DbNotification) => void> = [];
//   private activeListens: Map<string, () => Promise<void>> = new Map();
//   private ownTransaction = false;

//   constructor(
//     private db: PGlite,
//     private txMutex: Mutex,
//     private getInTransaction: () => boolean,
//     private setInTransaction: (val: boolean) => void,
//   ) {}

//   async query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>> {
//     const trimmedText = text.trim().toUpperCase();

//     // Handle LISTEN command
//     const listenMatch = trimmedText.match(/^LISTEN\s+(\w+)/i);
//     if (listenMatch) {
//       const channel = listenMatch[1].toLowerCase();
//       await this.setupListen(channel);
//       return toPgResult({ rows: [] as R[], affectedRows: 0 });
//     }

//     // Handle UNLISTEN command
//     const unlistenMatch = trimmedText.match(/^UNLISTEN\s+(\w+)/i);
//     if (unlistenMatch) {
//       const channel = unlistenMatch[1].toLowerCase();
//       await this.teardownListen(channel);
//       return toPgResult({ rows: [] as R[], affectedRows: 0 });
//     }

//     // Handle BEGIN - acquire mutex
//     if (trimmedText.startsWith("BEGIN")) {
//       await this.txMutex.acquire();
//       this.ownTransaction = true;
//       this.setInTransaction(true);
//       const result = await this.db.query<R>(text, values as any[]);
//       return toPgResult(result);
//     }

//     // Handle COMMIT/ROLLBACK - release mutex
//     if (trimmedText === "COMMIT" || trimmedText === "ROLLBACK") {
//       try {
//         const result = await this.db.query<R>(text, values as any[]);
//         return toPgResult(result);
//       } finally {
//         if (this.ownTransaction) {
//           this.ownTransaction = false;
//           this.setInTransaction(false);
//           this.txMutex.release();
//         }
//       }
//     }

//     // If no parameters and multiple statements, use exec()
//     if ((!values || values.length === 0) && hasMultipleStatements(text)) {
//       const results = await this.db.exec(text);
//       const lastResult = results[results.length - 1] || { rows: [], affectedRows: 0 };
//       return toPgResult(lastResult as { rows: R[]; affectedRows?: number });
//     }

//     const result = await this.db.query<R>(text, values as any[]);
//     return toPgResult(result);
//   }

//   private async setupListen(channel: string): Promise<void> {
//     if (this.activeListens.has(channel)) {
//       return;
//     }

//     const unsub = await this.db.listen(channel, (payload) => {
//       const notification: DbNotification = {
//         channel,
//         payload,
//         processId: 0,
//       };
//       this.notificationListeners.forEach((listener) => listener(notification));
//     });

//     this.activeListens.set(channel, unsub);
//   }

//   private async teardownListen(channel: string): Promise<void> {
//     const unsub = this.activeListens.get(channel);
//     if (unsub) {
//       await unsub();
//       this.activeListens.delete(channel);
//     }
//   }

//   on(event: "notification", callback: (msg: DbNotification) => void): this {
//     if (event === "notification") {
//       this.notificationListeners.push(callback);
//     }
//     return this;
//   }

//   off(event: "notification", callback: (msg: DbNotification) => void): this {
//     if (event === "notification") {
//       const idx = this.notificationListeners.indexOf(callback);
//       if (idx >= 0) {
//         this.notificationListeners.splice(idx, 1);
//       }
//     }
//     return this;
//   }

//   release(): void {
//     // Release mutex if we own a transaction
//     if (this.ownTransaction) {
//       this.ownTransaction = false;
//       this.setInTransaction(false);
//       this.txMutex.release();
//     }

//     this.activeListens.forEach(async (unsub) => {
//       try {
//         await unsub();
//       } catch {
//         // Ignore errors during cleanup
//       }
//     });
//     this.activeListens.clear();
//     this.notificationListeners = [];
//   }
// }

// /**
//  * Create a PGlite-backed adapter for testing.
//  */
// export function createPGliteAdapter(): IDbAdapter {
//   return new PGliteAdapter();
// }

// // Re-export IsolationLevel for convenience
// export { IsolationLevel };
