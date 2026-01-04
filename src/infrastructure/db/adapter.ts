
/**
 * Isolation levels for database transactions.
 */
export enum IsolationLevel {
  Serializable = "SERIALIZABLE",
  RepeatableRead = "REPEATABLE READ",
  ReadCommitted = "READ COMMITTED",
  ReadUncommitted = "READ UNCOMMITTED",
  SerializableRO = "SERIALIZABLE READ ONLY",
  RepeatableReadRO = "REPEATABLE READ READ ONLY",
  ReadCommittedRO = "READ COMMITTED READ ONLY",
}

/**
 * Notification message format (matching pg's Notification interface).
 */
// export interface DbNotification {
//   channel: string;
//   payload?: string;
//   processId?: number;
// }

// /**
//  * A minimal interface for database clients.
//  */
// export interface IDbClient {
//   query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>>;
//   release(): void;
//   on(event: "notification", callback: (msg: DbNotification) => void): this;
//   off(event: "notification", callback: (msg: DbNotification) => void): this;
// }

// /**
//  * Transaction context passed to transaction callbacks.
//  */
// export interface ITransactionClient extends IDbClient {
//   _inTransaction: true;
// }

// /**
//  * A minimal interface for database adapters.
//  * This allows swapping between pg Pool and PGlite for testing.
//  */
// export interface IDbAdapter {
//   /**
//    * Execute a query directly on the pool.
//    */
//   query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>>;

//   /**
//    * Get a client from the pool.
//    */
//   connect(): Promise<IDbClient>;

//   /**
//    * Execute a callback within a database transaction.
//    * The transaction is committed if the callback resolves successfully,
//    * or rolled back if it throws.
//    */
//   transaction<T>(
//     isolationLevel: IsolationLevel,
//     callback: (client: ITransactionClient) => Promise<T>
//   ): Promise<T>;

// }

// /**
//  * Adapter wrapping a pg Pool.
//  */
// export class PgAdapter implements IDbAdapter {
//   constructor(private pool: Pool) {}

//   async query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>> {
//     return this.pool.query<R>(text, values);
//   }

//   async connect(): Promise<IDbClient> {
//     const client = await this.pool.connect();
//     return new PgClientWrapper(client);
//   }

//   async transaction<T>(
//     isolationLevel: IsolationLevel,
//     callback: (client: ITransactionClient) => Promise<T>
//   ): Promise<T> {
//     const poolClient = await this.pool.connect();
//     const client = new PgClientWrapper(poolClient) as ITransactionClient;
//     (client as any)._inTransaction = true;

//     try {
//       await poolClient.query(`BEGIN ISOLATION LEVEL ${isolationLevel}`);
//       const result = await callback(client);
//       await poolClient.query("COMMIT");
//       return result;
//     } catch (error) {
//       await poolClient.query("ROLLBACK");
//       throw error;
//     } finally {
//       poolClient.release();
//     }
//   }

// }

// /**
//  * Wrapper around pg PoolClient to implement IDbClient.
//  */
// class PgClientWrapper implements IDbClient {
//   constructor(private client: PoolClient) {}

//   async query<R extends QueryResultRow = QueryResultRow>(
//     text: string,
//     values?: unknown[]
//   ): Promise<QueryResult<R>> {
//     return this.client.query<R>(text, values);
//   }

//   release(): void {
//     this.client.release();
//   }

//   on(event: "notification", callback: (msg: DbNotification) => void): this {
//     this.client.on(event, callback as any);
//     return this;
//   }

//   off(event: "notification", callback: (msg: DbNotification) => void): this {
//     this.client.off(event, callback as any);
//     return this;
//   }
// }

// /**
//  * Create a pg-based adapter from a Pool.
//  */
// export function createPgAdapter(pool: Pool): IDbAdapter {
//   return new PgAdapter(pool);
// }

// /**
//  * Type guard to check if the scope is a transaction client.
//  */
// export function isTransactionClient(scope: IDbClient | IDbAdapter): scope is ITransactionClient {
//   return (scope as any)._inTransaction === true;
// }
