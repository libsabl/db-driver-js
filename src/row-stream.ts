// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import EventEmitter from 'events';
import { CallbackPromise, promise } from '@sabl/async';
import { CanceledCallback, Canceler } from '@sabl/context';
import { ColumnInfo, PlainObject, Rows, Row } from '@sabl/db-api';

import type { DriverRows } from './driver';

/** Controller interface for a RowStream */
export interface RowController {
  /**
   * Awaitable method which will either resolve when
   * setColumns() or end() is called, or will reject
   * when err() is called. Useful for implementers
   * in implementations of DbApi.query
   */
  ready(): Promise<Error | null>;

  /** Set the column information. Must be called before pushing rows */
  setColumns(columns: ColumnInfo[]): void;

  /** Push a row object that implements the {@link Row} interface */
  pushRow(row: Row): void;

  /** Push a row that is a plain array of values */
  pushArray(row: any[]): void;

  /** Push a row that is a plain object */
  pushObject(row: PlainObject): void;

  /**
   * Indicate the query operation encountered an error.
   * Will automatically call end()
   */
  error(err: unknown): void;

  /** Indicate all data has been received */
  end(): void;

  on(event: 'pause' | 'resume' | 'cancel', fn: () => void): void;
  off(event: 'pause' | 'resume' | 'cancel', fn: () => void): void;
}

export interface RowStreamOptions {
  /** Raise `pause` event if number of buffered rows reaches this amount  */
  pauseCount: number;

  /** Raise `resume` event if number of buffered rows falls back below this amount */
  resumeCount?: number;
}

export function asError(err: unknown): Error | null {
  if (err == null) {
    return null;
  }
  if (err instanceof Error) {
    return err;
  }
  if (typeof err === 'object') {
    if ('message' in err) {
      return new Error(String((<any>err).message));
    }
  }
  return new Error(String(err));
}

/**
 * A buffered implementation of {@link Rows}. Useful for wrapping
 * platform APIs that do not support cursors, but instead push rows
 * using callbacks or events.
 *
 * Implementers must use the controller API to set the column info,
 * push rows, and signal when all rows have been received. If a
 * platform supports, pausing and resuming or canceling an ongoing
 * query, it can listen for the for `pause`, `resume`, and `cancel`
 * events on the controller.
 *
 * This implementation will emit `complete` as soon as the controller
 * signals `end()`, even if the client Rows are still open. This allows
 * the underlying connection to be released, even while a client may
 * continue to iterate over buffered rows.
 */
export class RowStream extends EventEmitter implements DriverRows {
  static readonly #Controller = class implements RowController {
    readonly #stream: RowStream;
    constructor(stream: RowStream) {
      this.#stream = stream;
    }

    ready(): Promise<Error | null> {
      if (this.#stream.#ready) {
        return Promise.resolve(this.#stream.err);
      }
      if (this.#stream.#waitReady == null) {
        this.#stream.#waitReady = promise<Error | null>();
      }
      return this.#stream.#waitReady;
    }

    setColumns(columns: ColumnInfo[]): void {
      this.#stream.#setColumns(columns);
    }

    pushRow(row: Row): void {
      this.#stream.#ensureFields('pushRow');
      this.#stream.#pushRow(row);
    }

    pushArray(row: any[]): void {
      this.#stream.#ensureFields('pushArray');
      this.#stream.#pushArray(row);
    }

    pushObject(row: PlainObject): void {
      this.#stream.#ensureFields('pushObject');
      this.#stream.#pushObject(row);
    }

    end(): void {
      this.#stream.#end();
    }

    error(err: unknown): void {
      this.#stream.#error(err);
    }

    on(event: 'pause' | 'resume' | 'cancel', fn: () => void): void {
      this.#stream.on(event, fn);
    }

    off(event: 'pause' | 'resume' | 'cancel', fn: () => void): void {
      this.#stream.off(event, fn);
    }
  };

  /** Check whether a RowStream is closed */
  static isClosed(rs: Rows): boolean {
    if (rs instanceof RowStream) {
      return rs.#closed;
    }
    throw new Error('rows is not a RowStream');
  }

  /** Check the size of a RowStream buffer */
  static size(rs: Rows): number {
    if (rs instanceof RowStream) {
      return rs.#buf.length;
    }
    throw new Error('rows is not a RowStream');
  }

  /** Check the stats of a RowStream buffer */
  static stats(rs: Rows): {
    ready: boolean;
    size: number;
    paused: boolean;
    canPause: boolean;
    pauseCount: number | undefined;
    resumeCount: number | undefined;
  } {
    if (rs instanceof RowStream) {
      return {
        ready: rs.#ready,
        size: rs.#buf.length,
        paused: rs.#paused,
        canPause: rs.#canPause,
        pauseCount: rs.#pauseCount,
        resumeCount: rs.#resumeCount,
      };
    }
    throw new Error('rows is not a RowStream');
  }

  readonly #controller: RowController;
  readonly #buf: Row[] = [];
  readonly #pauseCount?: number;
  readonly #resumeCount?: number;
  readonly #canPause: boolean;

  #row: Row | null = null;
  #columns: ColumnInfo[] | null = null;
  #fieldNames: string[] | null = null;
  #err: Error | null = null;
  #ready = false;

  // Done means the *controller* has indicated
  // that all data has been received
  #done = false;

  // Closing means the *client* has requested that
  // the rows be closed, but closing has not yet
  // resolved.
  #closing = false;

  // Closed means the *client* has requested that
  // the rows be closed AND and operations have
  // been flushed.
  #closed = false;

  // Canceling means the stream is being canceled,
  // either by the context that created it or
  // because the controller signalled an error
  #canceling = false;

  #paused = false;

  #waitReady: CallbackPromise<Error | null> | null = null;
  #waitNext: CallbackPromise<boolean> | null = null;
  #waitClose: CallbackPromise<void> | null = null;

  #onCancel: CanceledCallback | null = null;
  #clr?: Canceler;

  /** Create a new non-cancelable, non-pausable row stream */
  constructor();

  /** Create a new cancelable row stream */
  constructor(clr: Canceler, options?: RowStreamOptions);

  /** Create new non-cancelable with the provided buffering options */
  constructor(options: RowStreamOptions);

  constructor(
    clrOrOptions?: Canceler | RowStreamOptions,
    options?: RowStreamOptions
  ) {
    super();

    let clr: Canceler | undefined;
    if (clrOrOptions != null) {
      if ('onCancel' in clrOrOptions) {
        clr = clrOrOptions;
      } else {
        options = clrOrOptions;
      }
    }

    this.#controller = new RowStream.#Controller(this);

    const [canPause, pauseCount, resumeCount] = this.#validateOptions(options);
    this.#canPause = canPause;
    if (canPause) {
      this.#pauseCount = pauseCount;
      this.#resumeCount = resumeCount;
    }

    if (clr != null) {
      this.#clr = clr;
      clr.onCancel((this.#onCancel = this.#cancel.bind(this)));
    }
  }

  /** The controller for this row stream. */
  get controller(): RowController {
    return this.#controller;
  }

  #validateOptions(options?: RowStreamOptions): [boolean, number?, number?] {
    if (options == null) {
      return [false];
    }

    const pauseCount = options.pauseCount;
    if (pauseCount == null) {
      if (options.resumeCount != null) {
        throw new Error('pauseCount must be provided with resumeCount');
      }
      return [false];
    }

    if (pauseCount < 2) {
      throw new Error('pauseCount must more than 1');
    }
    let resumeCount = options.resumeCount;
    if (resumeCount == null) {
      resumeCount = Math.floor(pauseCount / 2);
    } else {
      if (resumeCount < 0) {
        throw new Error('resumeCount cannot be negative');
      } else if (resumeCount >= pauseCount) {
        throw new Error('resumeCount must be less than pauseCount');
      }
    }

    return [true, pauseCount, resumeCount];
  }

  #cancel(err: Error): void {
    if (this.#canceling) {
      return;
    }
    this.#canceling = true;
    this.#err = err;

    if (this.#onCancel != null) {
      this.#clr!.off(this.#onCancel);
      this.#onCancel = null;
    }
    this.emit('cancel');

    if (!this.#ready) {
      this.#resolveReady(err);
    }

    const wnx = this.#waitNext;
    if (wnx != null) {
      this.#waitNext = null;
      wnx.reject(err);
    }

    this.#end();
  }

  #setColumns(columns: ColumnInfo[]) {
    this.#columns = columns;
    this.#fieldNames = columns.map((c) => c.name);
    if (!this.#ready) {
      this.#resolveReady(null);
    }
  }

  #resolveReady(err: Error | null) {
    this.#ready = true;
    const wReady = this.#waitReady;
    if (wReady != null) {
      this.#waitReady = null;
      wReady.resolve(err);
    }
  }

  #ensureFields(op: string): string[] {
    if (this.#fieldNames == null) {
      throw new Error(`Cannot ${op}: Column info not yet set`);
    }
    return this.#fieldNames;
  }

  #pushArray(row: any[]): void {
    this.#pushRow(Row.fromArray(row, this.#fieldNames!));
  }

  #pushObject(row: PlainObject): void {
    this.#pushRow(Row.fromObject(row, this.#fieldNames!));
  }

  #pushRow(row: Row) {
    if (this.#canceling) {
      // Ignore the row. Query is canceling
      return;
    }
    const wnx = this.#waitNext;

    const buf = this.#buf;
    if (buf.length == 0 && wnx != null) {
      // Nothing in buffer, and next already waiting
      // Immediately set the row and resolve the next
      this.#waitNext = null;
      this.#row = row;
      wnx.resolve(true);
      return;
    }

    // No next is waiting or client is still
    // working through the buffer. Add
    // this row to the buffer.

    buf.push(row);
    if (this.#canPause) {
      if (!this.#paused) {
        if (buf.length >= this.#pauseCount!) {
          this.#pause();
        }
      }
    }
  }

  #error(err: unknown) {
    this.#err =
      asError(err) || new Error('Underlying stream encountered an error');
    this.#cancel(this.#err);
    this.#end();
  }

  #end() {
    this.#done = true;

    if (!this.#ready) {
      // End with no data. Set columns
      // to empty array to resolve ready
      this.#setColumns([]);
    }

    // Release the underlying connection
    this.emit('complete', null, [this]);

    const wc = this.#waitClose;
    if (wc != null) {
      // Someone was waiting for the stream to finish
      this.#closed = true;
      this.#closing = false;
      this.#waitClose = null;
      wc.resolve();
    } else if (this.#err != null) {
      // Ending due to error
      // Mark as closed
      this.#closed = true;
      this.#closing = false;
    }

    const wnx = this.#waitNext;
    if (wnx != null && this.#buf.length == 0) {
      // No more data and there is a pending next().
      // Run close(). to clear it out and close the Rows.
      this.close();
    }
  }

  #pause() {
    this.#paused = true;
    this.emit('pause');
  }

  #resume() {
    this.#paused = false;
    this.emit('resume');
  }

  async close(): Promise<void> {
    if (this.#closed) {
      return;
    }
    if (this.#closing) {
      return this.#waitClose!;
    }
    this.#closing = true;

    if (!this.#ready) {
      this.#resolveReady(null);
    }

    // Already closing by choice.
    // Clear any cancel callback
    if (this.#onCancel != null) {
      this.#clr!.off(this.#onCancel);
      this.#onCancel = null;
    }

    const wnx = this.#waitNext;

    if (this.#done) {
      // Controller is done, and no next call is waiting
      // close immediately and return.
      this.#closed = true;
      this.#closing = false;

      if (wnx != null) {
        // There was a pending next(). Resolve to to false
        this.#waitNext = null;
        wnx.resolve(false);
      }
      return;
    } else {
      // Controller not yet done. Tell it to cancel
      if (!this.#canceling) {
        this.#canceling = true;
        this.emit('cancel');
      }
    }

    const waitClose = (this.#waitClose = promise());
    if (wnx != null) {
      // There was a pending next(). Resolve it to false
      // whenever close itself is resolved
      this.#waitNext = null;
      waitClose.then(() => wnx.resolve(false));
    }
    return waitClose;
  }

  async next(): Promise<boolean> {
    if (this.#closed) {
      // Client has terminated iteration
      return false;
    } else if (this.#closing) {
      // Closing is waiting for connection to stop
      await this.close();
      return false;
    }
    if (this.#waitNext != null) {
      throw new Error('Existing next() call has not yet resolved');
    }

    const buf = this.#buf;

    if (buf.length > 0) {
      // Data is already buffered
      this.#row = buf.shift()!;

      // Signal resume if applicable
      if (this.#canPause && this.#paused) {
        if (buf.length <= this.#resumeCount!) {
          this.#resume();
        }
      }

      return true;
    }

    if (this.#done) {
      // All data is buffered. There will be no more rows.
      // Automatically close the rows per Rows contract
      await this.close();
      return false;
    }

    return (this.#waitNext = promise<boolean>());
  }

  get columns(): string[] {
    if (this.#fieldNames == null) {
      throw new Error('Column information not yet available');
    }
    return this.#fieldNames.slice();
  }

  get columnTypes(): ColumnInfo[] {
    if (this.#columns == null) {
      throw new Error('Column information not yet available');
    }
    return this.#columns.slice();
  }

  get err(): Error | null {
    return this.#err;
  }

  get row(): Row {
    if (this.#row == null) {
      throw new Error('No row loaded. Call next()');
    }

    return this.#row;
  }

  async *[Symbol.asyncIterator](): AsyncIterator<Row, any, undefined> {
    try {
      while (await this.next()) {
        yield this.row;
      }
    } finally {
      await this.close();
    }
  }
}
