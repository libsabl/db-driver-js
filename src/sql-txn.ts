// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import EventEmitter from 'events';
import { StorageKind, StorageMode } from '@sabl/storage-pool';
import { IsolationLevel, TxnOptions } from '@sabl/txn';

// Type-only imports:
import type { IContext } from '@sabl/context';
import type { DbConn, DbTxn, Result, Rows, Row } from '@sabl/db-api';
import type { DriverTxn } from './driver';

/** Optional config interface to control the behavior of {@link SqlTxn} */
export interface SqlTxnConfig {
  /**
   * Implement to indicate whether explicit READ ONLY / READ WRITE
   * mode is supported.
   */
  readonly supportsReadOnly?: boolean;

  /**
   * Implement to override default 'START TRANSACTION' statement
   */
  readonly startTransactionVerb?: string;

  /**
   * Implement to indicate which isolation levels are supported.
   * IsolationLevel.default is always supported and skips SET
   * TRANSACTION ISOLATION LEVEL statement.
   */
  supportsIsolationLevel?(level: IsolationLevel): boolean;

  /**
   * Implement to support nested transactions.
   */
  beginNestedTxn?(
    con: DbConn,
    ctx: IContext,
    opts?: TxnOptions
  ): Promise<DbTxn>;
}

/**
 * Generic implementation for the lifecycle of a transaction
 * which executes ANSI SQL commands on the connection provided
 * to the constructor to set transaction options,
 * start the transaction, and commit it or roll it back.
 */
export class SqlTxn extends EventEmitter implements DriverTxn {
  #closed = false;
  #started = false;
  readonly #opts: TxnOptions | undefined;
  readonly #con: DbConn;
  readonly #config: SqlTxnConfig;
  readonly #ctx: IContext;

  /** Create and start a {@link SqlTxn} */
  static async start(
    ctx: IContext,
    txnCon: DbConn,
    opts?: TxnOptions,
    config?: SqlTxnConfig
  ): Promise<DriverTxn> {
    const txn = new SqlTxn(ctx, txnCon, opts, config);
    await txn.start();
    return txn;
  }

  constructor(
    ctx: IContext,
    txnCon: DbConn,
    opts?: TxnOptions,
    config?: SqlTxnConfig
  ) {
    super();
    this.#ctx = ctx;
    this.#con = txnCon;
    this.#opts = opts;
    this.#config = config || {};

    if ('beginNestedTxn' in this.#config) {
      Object.defineProperty(this, 'beginTxn', {
        value: this._beginTxn,
        configurable: false,
      });
    }
  }

  get mode(): StorageMode {
    return StorageMode.txn;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  #checkStatus() {
    if (this.#closed) {
      throw new Error('Transaction is already closed');
    }
    if (!this.#started) {
      throw new Error('Transaction is not yet started');
    }
  }

  supportsIsolationLevel(level: IsolationLevel): boolean {
    if (level === IsolationLevel.default) {
      return true;
    }
    if (typeof this.#config.supportsIsolationLevel === 'function') {
      return this.#config.supportsIsolationLevel(level);
    }
    return false;
  }

  supportsReadOnly(): boolean {
    return this.#config.supportsReadOnly === true;
  }

  /**
   * Get the SQL key word(s) for the provided
   * isolation level.
   */
  isolationLevelKeyword(level: IsolationLevel): string {
    switch (level) {
      case IsolationLevel.readUncommitted:
        return 'READ UNCOMMITTED';
      case IsolationLevel.readCommitted:
        return 'READ COMMITTED';
      case IsolationLevel.writeCommitted:
        return 'WRITE COMMITTED';
      case IsolationLevel.repeatableRead:
        return 'REPEATABLE READ';
      case IsolationLevel.snapshot:
        return 'SNAPSHOT';
      case IsolationLevel.serializable:
        return 'SERIALIZABLE';
      case IsolationLevel.linearizable:
        return 'LINEARIZABLE';
      default:
        throw new Error('Unsupported isolation level');
    }
  }

  async start(): Promise<void> {
    const con = this.#con;
    const opts = this.#opts;
    const ctx = this.#ctx;

    try {
      if (opts != null) {
        const level = opts.isolationLevel;
        if (level != null && level != IsolationLevel.default) {
          if (!this.supportsIsolationLevel(level)) {
            throw new Error('Unsupported isolation level');
          }
          const keyWord = this.isolationLevelKeyword(level);
          await con.exec(ctx, `SET TRANSACTION ISOLATION LEVEL ${keyWord}`);
        }
      }

      let startCmd = this.#config.startTransactionVerb || 'START TRANSACTION';
      if (opts != null && typeof opts.readOnly === 'boolean') {
        if (this.supportsReadOnly()) {
          startCmd += opts.readOnly === true ? ' READ ONLY' : ' READ WRITE';
        } else if (opts.readOnly === true) {
          throw new Error('Read only transactions not supported');
        } else {
          // Just ignoring readOnly === false if underlying
          // driver does not support readOnly mode
        }
      }

      await con.exec(ctx, startCmd);

      this.#started = true;
      return;
    } catch (err) {
      this.#closed = true;
      throw err;
    }
  }

  async exec(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Result> {
    this.#checkStatus();
    return this.#con.exec(ctx, sql, ...params);
  }

  async queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    this.#checkStatus();
    return this.#con.queryRow(ctx, sql, ...params);
  }

  async query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    this.#checkStatus();
    return this.#con.query(ctx, sql, ...params);
  }

  async commit(): Promise<void> {
    this.#checkStatus();
    this.#closed = true;
    const ctx = this.#ctx;

    try {
      await this.#con.exec(ctx, 'COMMIT');
    } finally {
      this.emit('complete', this);
    }
  }

  async rollback(): Promise<void> {
    this.#checkStatus();
    this.#closed = true;
    const ctx = this.#ctx;

    try {
      await this.#con.exec(ctx, 'ROLLBACK');
    } finally {
      this.emit('complete', this);
    }
  }

  protected _beginTxn(
    ctx: IContext,
    opts?: TxnOptions | undefined
  ): Promise<DbTxn> {
    return this.#config.beginNestedTxn!(this.#con, ctx, opts);
  }
}
