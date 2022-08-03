// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

// All type-only imports:
import type { IContext } from '@sabl/context';
import type { TxnOptions } from '@sabl/txn';
import type {
  Row,
  DbTxn,
  ParamValue,
  Result,
  Rows,
  ColumnInfo,
  PlainObject,
} from '@sabl/db-api';

/** An {@link EventEmitter} that supports a `'complete'` event */
export interface CompleteEmitter {
  /**
   * Schedule a callback to be run when the
   * transaction has completed all operations
   */
  on(type: 'complete', fn: () => void): void;

  /** Remove a scheduled 'complete' callback */
  off(type: 'complete', fn: () => void): void;
}

/**
 * A {@link DbTxn} which emits a `'complete'` event
 * when all work is completed
 */
export interface DriverTxn extends DbTxn, CompleteEmitter {}

/**
 * A {@link Rows} which emits a `'complete'` event when
 * the underlying cursor has been closed
 */
export interface DriverRows extends Rows, CompleteEmitter {}

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

/**
 * Derive an array of ColumnInfo from a plain object,
 * using `Object.keys`, and `typeof`. All columns are assumed
 * to be non-nullable unless the value itself is null.
 * If a property value is null, its type name will
 * be 'unknown'
 */
export function parseCols(obj: PlainObject): ColumnInfo[] {
  const cols: ColumnInfo[] = [];
  for (const k of Object.keys(obj)) {
    const v = obj[k];
    let type = v == null ? 'unknown' : typeof v;
    if (type === 'object') {
      if (v instanceof Date) {
        type = 'datetime';
      } else if (v instanceof Uint8Array) {
        type = 'binary';
      }
    }
    cols.push({
      name: k,
      typeName: type,
      nullable: v == null,
    });
  }

  return cols;
}
