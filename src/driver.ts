// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

// All type-only imports:
import type { IContext } from '@sabl/context';
import type { TxnOptions } from '@sabl/txn';
import type { Row, DbTxn, ParamValue, Result, Rows } from '@sabl/db-api';

/** An {@link EventEmitter} that supports a `'complete'` event */
export interface CompleteEmitter<T> {
  /**
   * Schedule a callback to be run when the
   * transaction has completed all operations
   */
  on(type: 'complete', fn: (item: T) => any | Promise<any>): void;

  /** Remove a scheduled 'complete' callback */
  off(type: 'complete', fn: (item: T) => any | Promise<any>): void;
}

/**
 * A {@link DbTxn} which emits a `'complete'` event
 * when all work is completed
 */
export interface DriverTxn extends DbTxn, CompleteEmitter<DbTxn> {}

/**
 * A {@link Rows} which emits a `'complete'` event when
 * the underlying cursor has been closed
 */
export interface DriverRows extends Rows, CompleteEmitter<Rows> {}

/**
 * A {@link DbConn} which returns a {@link DriverRows}
 * from `query`, and a {@link DriverTxn} from `beginTxn`.
 * {@link DriverConn} is used by QueueConn, and does not
 * need to implement its own internal queueing or open/close
 * status checking.
 */
export interface DriverConn {
  exec(ctx: IContext, sql: string, ...params: ParamValue[]): Promise<Result>;

  queryRow(
    ctx: IContext,
    sql: string,
    ...params: ParamValue[]
  ): Promise<Row | null>;

  query(
    ctx: IContext,
    sql: string,
    ...params: ParamValue[]
  ): Promise<DriverRows>;

  beginTxn(ctx: IContext, opts?: TxnOptions): Promise<DriverTxn>;
}
