// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { wait } from '@sabl/async';
import { Context, IContext } from '@sabl/context';
import { DbConn, DbTxn, PlainObject, Result, Row, Rows } from '@sabl/db-api';
import { StorageKind, StorageMode } from '@sabl/storage-pool';
import { IsolationLevel, Transactable, TxnOptions } from '@sabl/txn';

import { RowStream, SqlTxn } from '$';

interface Cmd {
  method: string;
  sql?: string;
  params?: unknown[];
}

class MockConn implements DbConn {
  readonly commands: Cmd[] = [];
  readonly delay: number;

  constructor(delay = 0) {
    this.delay = delay;
  }

  get mode(): StorageMode {
    return StorageMode.conn;
  }

  get kind(): string {
    return StorageKind.rdb;
  }

  async close(): Promise<void> {
    this.commands.push({ method: 'close' });
    return Promise.resolve();
  }

  async exec(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Result> {
    if (this.delay > 0) {
      await wait(this.delay);
    }
    this.commands.push({ method: 'exec', sql, params });
    return Promise.resolve({ rowsAffected: 0, lastId: 0 });
  }

  async queryRow(
    ctx: IContext,
    sql: string,
    ...params: unknown[]
  ): Promise<Row | null> {
    if (this.delay > 0) {
      await wait(this.delay);
    }
    this.commands.push({ method: 'queryRow', sql, params });
    return Promise.resolve(null);
  }

  async query(ctx: IContext, sql: string, ...params: unknown[]): Promise<Rows> {
    if (this.delay > 0) {
      await wait(this.delay);
    }
    this.commands.push({ method: 'query', sql, params });
    const rows = new RowStream();
    rows.controller.end();
    return Promise.resolve(rows);
  }

  async beginTxn(): Promise<DbTxn> {
    throw new Error('Method not implemented.');
  }
}

describe('start', () => {
  it('uses default verb', async () => {
    const con = new MockConn(1);
    const txn = new SqlTxn(Context.background, con);

    await txn.start();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
    ]);
  });

  it('uses specified verb', async () => {
    const con = new MockConn(1);
    const txn = new SqlTxn(Context.background, con, undefined, {
      startTransactionVerb: 'BEGIN TRANSACTION',
    });

    await txn.start();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'BEGIN TRANSACTION',
        params: [],
      },
    ]);
  });

  it('sets isolation level', async () => {
    const cases: PlainObject = {
      [IsolationLevel.readUncommitted]: 'READ UNCOMMITTED',
      [IsolationLevel.readCommitted]: 'READ COMMITTED',
      [IsolationLevel.writeCommitted]: 'WRITE COMMITTED',
      [IsolationLevel.repeatableRead]: 'REPEATABLE READ',
      [IsolationLevel.snapshot]: 'SNAPSHOT',
      [IsolationLevel.serializable]: 'SERIALIZABLE',
      [IsolationLevel.linearizable]: 'LINEARIZABLE',
    };

    for (const level in cases) {
      const con = new MockConn(1);
      const keyword = cases[level];

      const txn = new SqlTxn(
        Context.background,
        con,
        { isolationLevel: +level },
        {
          supportsIsolationLevel() {
            return true;
          },
        }
      );

      await txn.start();

      expect(con.commands).toEqual([
        {
          method: 'exec',
          sql: `SET TRANSACTION ISOLATION LEVEL ${keyword}`,
          params: [],
        },
        {
          method: 'exec',
          sql: 'START TRANSACTION',
          params: [],
        },
      ]);
    }
  });

  it('sets read-only keyword', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(
      Context.background,
      con,
      { readOnly: true },
      { supportsReadOnly: true }
    );

    await txn.start();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION READ ONLY',
        params: [],
      },
    ]);
  });

  it('sets read-write keyword', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(
      Context.background,
      con,
      { readOnly: false },
      { supportsReadOnly: true }
    );

    await txn.start();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION READ WRITE',
        params: [],
      },
    ]);
  });

  it('ignores readOnly = false', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(
      Context.background,
      con,
      { readOnly: false },
      { supportsReadOnly: false }
    );

    await txn.start();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
    ]);
  });

  it('ignores default isolation level', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(
      Context.background,
      con,
      { isolationLevel: IsolationLevel.default },
      {
        supportsIsolationLevel() {
          return false;
        },
      }
    );

    await txn.start();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
    ]);
  });

  it('rejects unsupported isolation level', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(Context.background, con, {
      isolationLevel: IsolationLevel.readCommitted,
    });

    await expect(txn.start()).rejects.toThrow('Unsupported isolation level');

    expect(con.commands).toEqual([]);
  });

  it('rejects unknown isolation level', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(
      Context.background,
      con,
      {
        isolationLevel: 123,
      },
      {
        supportsIsolationLevel() {
          return true;
        },
      }
    );

    await expect(txn.start()).rejects.toThrow('Unsupported isolation level');

    expect(con.commands).toEqual([]);
  });

  it('rejects unsupported readOnly option', async () => {
    const con = new MockConn(1);

    const txn = new SqlTxn(Context.background, con, {
      readOnly: true,
    });

    await expect(txn.start()).rejects.toThrow(
      'Read only transactions not supported'
    );

    expect(con.commands).toEqual([]);
  });

  it('static - awaits start()', async () => {
    const con = new MockConn(1);

    await SqlTxn.start(Context.background, con);

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
    ]);
  });

  it('static - rejects if start() fails', async () => {
    const con = new MockConn(1);

    expect(
      SqlTxn.start(Context.background, con, {
        isolationLevel: IsolationLevel.readCommitted,
      })
    ).rejects.toThrow('Unsupported isolation level');

    expect(con.commands).toEqual([]);
  });
});

describe('exec', () => {
  it('invokes exec on connection', async () => {
    const ctx = Context.background;
    const sql = 'INSERT 1 INTO 2';
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);

    await txn.exec(ctx, sql, 1, 2, 3);

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: sql,
        params: [1, 2, 3],
      },
    ]);
  });

  it('rejects if not yet started', async () => {
    const ctx = Context.background;
    const sql = 'INSERT 1 INTO 2';
    const con = new MockConn(1);
    const txn = new SqlTxn(ctx, con);

    await expect(() => txn.exec(ctx, sql, 1, 2, 3)).rejects.toThrow(
      'Transaction is not yet started'
    );

    expect(con.commands).toEqual([]);
  });

  it('rejects if already closed', async () => {
    const ctx = Context.background;
    const sql = 'INSERT 1 INTO 2';
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);
    await txn.commit();

    await expect(() => txn.exec(ctx, sql, 1, 2, 3)).rejects.toThrow(
      'Transaction is already closed'
    );

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'COMMIT',
        params: [],
      },
    ]);
  });
});

describe('queryRow', () => {
  it('invokes queryRow on connection', async () => {
    const ctx = Context.background;
    const sql = 'SELECT 1, 2';
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);

    await txn.queryRow(ctx, sql, 1, 2, 3);

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'queryRow',
        sql: sql,
        params: [1, 2, 3],
      },
    ]);
  });

  it('rejects if not yet started', async () => {
    const ctx = Context.background;
    const sql = 'SELECT 1, 2';
    const con = new MockConn(1);
    const txn = new SqlTxn(ctx, con);

    await expect(() => txn.queryRow(ctx, sql, 1, 2, 3)).rejects.toThrow(
      'Transaction is not yet started'
    );

    expect(con.commands).toEqual([]);
  });

  it('rejects if already closed', async () => {
    const ctx = Context.background;
    const sql = 'SELECT 1, 2';
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);
    await txn.commit();

    await expect(() => txn.queryRow(ctx, sql, 1, 2, 3)).rejects.toThrow(
      'Transaction is already closed'
    );

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'COMMIT',
        params: [],
      },
    ]);
  });
});

describe('query', () => {
  it('invokes query on connection', async () => {
    const ctx = Context.background;
    const sql = 'SELECT 1, 2';
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);

    await txn.query(ctx, sql, 1, 2, 3);

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'query',
        sql: sql,
        params: [1, 2, 3],
      },
    ]);
  });

  it('rejects if not yet started', async () => {
    const ctx = Context.background;
    const sql = 'SELECT 1, 2';
    const con = new MockConn(1);
    const txn = new SqlTxn(ctx, con);

    await expect(() => txn.query(ctx, sql, 1, 2, 3)).rejects.toThrow(
      'Transaction is not yet started'
    );

    expect(con.commands).toEqual([]);
  });

  it('rejects if already closed', async () => {
    const ctx = Context.background;
    const sql = 'SELECT 1, 2';
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);
    await txn.commit();

    await expect(() => txn.query(ctx, sql, 1, 2, 3)).rejects.toThrow(
      'Transaction is already closed'
    );

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'COMMIT',
        params: [],
      },
    ]);
  });
});

describe('commit', () => {
  it('invokes commit on connection', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);

    await txn.commit();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'COMMIT',
        params: [],
      },
    ]);
  });

  it('rejects if not yet started', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const txn = new SqlTxn(ctx, con);

    await expect(() => txn.commit()).rejects.toThrow(
      'Transaction is not yet started'
    );

    expect(con.commands).toEqual([]);
  });

  it('rejects if already closed', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);
    await txn.commit();

    await expect(() => txn.commit()).rejects.toThrow(
      'Transaction is already closed'
    );

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'COMMIT',
        params: [],
      },
    ]);
  });
});

describe('rollback', () => {
  it('invokes rollback on connection', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);

    await txn.rollback();

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'ROLLBACK',
        params: [],
      },
    ]);
  });

  it('rejects if not yet started', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const txn = new SqlTxn(ctx, con);

    await expect(() => txn.rollback()).rejects.toThrow(
      'Transaction is not yet started'
    );

    expect(con.commands).toEqual([]);
  });

  it('throws if already closed', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const txn = await SqlTxn.start(ctx, con);
    await txn.commit();

    await expect(() => txn.rollback()).rejects.toThrow(
      'Transaction is already closed'
    );

    expect(con.commands).toEqual([
      {
        method: 'exec',
        sql: 'START TRANSACTION',
        params: [],
      },
      {
        method: 'exec',
        sql: 'COMMIT',
        params: [],
      },
    ]);
  });
});

describe('supportsIsolationLevel', () => {
  it('always returns true for default', () => {
    const con = new MockConn(1);
    const txn = new SqlTxn(Context.background, con);
    expect(txn.supportsIsolationLevel(IsolationLevel.default)).toBe(true);
  });
});

describe('StorageApi', () => {
  it('returns kind = rdb', () => {
    const con = new MockConn(1);
    const txn = new SqlTxn(Context.background, con);
    expect(txn.kind).toEqual(StorageKind.rdb);
  });

  it('returns mode = txn', () => {
    const con = new MockConn(1);
    const txn = new SqlTxn(Context.background, con);
    expect(txn.mode).toEqual(StorageMode.txn);
  });
});

describe('nestedTxn', () => {
  it('detects beginNestedTxn in config', async () => {
    const ctx = Context.background;
    const con = new MockConn(1);
    const fakeTxn: any = {};

    const txn = await SqlTxn.start(ctx, con, undefined, {
      beginNestedTxn(con, ctx, opts?) {
        fakeTxn.args = { con, ctx, opts };
        return Promise.resolve(<DbTxn>(<unknown>fakeTxn));
      },
    });

    const childOptions: TxnOptions = { readOnly: false };

    const subTxn = await (<Transactable<DbTxn>>(<unknown>txn)).beginTxn(
      ctx,
      childOptions
    );

    expect(subTxn).toBe(fakeTxn);
    expect(fakeTxn.args.ctx).toBe(ctx);
    expect(fakeTxn.args.con).toBe(con);
    expect(fakeTxn.args.opts).toBe(childOptions);
  });
});
