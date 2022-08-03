// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { CanceledError, Canceler, Context } from '@sabl/context';
import { Row, Rows } from '@sabl/db-api';
import { wait } from '@sabl/async';
import { later, Timeline } from '@sabl/async-test';

import { RowController, RowStream, RowStreamOptions, DriverRows } from '$';
import { inspectPromise } from './fixtures/utils';

function newRows(): [DriverRows, RowController];
function newRows(
  clr: Canceler,
  options?: RowStreamOptions
): [DriverRows, RowController];
function newRows(options: RowStreamOptions): [DriverRows, RowController];
function newRows(
  clrOrOptions?: Canceler | RowStreamOptions,
  options?: RowStreamOptions
): [DriverRows, RowController] {
  const rs = new RowStream(<Canceler>clrOrOptions!, options);
  return [rs, rs.controller];
}

function setCols(ctrl: RowController) {
  ctrl.setColumns([
    { name: 'id', typeName: 'number', nullable: false },
    { name: 'code', typeName: 'string', nullable: false },
    { name: 'label', typeName: 'string', nullable: true },
  ]);
}

function putData(ctrl: RowController, cnt = 1, start = 0) {
  const end = start + cnt;
  for (let i = start; i < end; i++) {
    ctrl.pushArray([i + 1, 'the code', 'the label']);
  }
}

function range(cnt: number, start?: number) {
  start = start || 1;
  const end = start + cnt;
  const out: number[] = [];
  for (let i = start; i < end; i++) {
    out.push(i);
  }
  return out;
}

describe('RowStream', () => {
  describe('isClosed', () => {
    it('returns false for not closed rows', () => {
      const [rows] = newRows();
      expect(RowStream.isClosed(rows)).toBe(false);
    });

    it('returns true for not closed rows', async () => {
      const [rows, ctrl] = newRows();
      ctrl.end();
      await rows.close();
      expect(RowStream.isClosed(rows)).toBe(true);
    });

    it('throws for non-RowStream', () => {
      expect(() => RowStream.isClosed(<Rows>null!)).toThrow('not a RowStream');
    });
  });

  describe('size', () => {
    it('returns buffer size', async () => {
      const [rows, ctrl] = newRows();
      expect(RowStream.size(rows)).toBe(0);

      setCols(ctrl);
      putData(ctrl, 2);

      expect(RowStream.size(rows)).toBe(2);

      await rows.next();

      expect(RowStream.size(rows)).toBe(1);

      putData(ctrl, 3);
    });

    it('throws for non-RowStream', () => {
      expect(() => RowStream.size(<Rows>null!)).toThrow('not a RowStream');
    });
  });

  describe('stats', () => {
    it('returns stream stats', async () => {
      const [rows, ctrl] = newRows({ pauseCount: 5, resumeCount: 3 });
      expect(RowStream.stats(rows)).toEqual({
        ready: false,
        size: 0,
        paused: false,
        canPause: true,
        pauseCount: 5,
        resumeCount: 3,
      });

      setCols(ctrl);
      putData(ctrl, 6);

      expect(RowStream.stats(rows)).toEqual({
        ready: true,
        size: 6,
        paused: true,
        canPause: true,
        pauseCount: 5,
        resumeCount: 3,
      });

      await rows.next(); // 5
      await rows.next(); // 4
      await rows.next(); // 3
      await rows.next(); // 2

      expect(RowStream.stats(rows)).toEqual({
        ready: true,
        size: 2,
        paused: false,
        canPause: true,
        pauseCount: 5,
        resumeCount: 3,
      });
    });

    it('throws for non-RowStream', () => {
      expect(() => RowStream.stats(<Rows>null!)).toThrow('not a RowStream');
    });
  });

  describe('ctor:validateOptions', () => {
    it('requires pauseCount with resumeCount', () => {
      expect(
        () => new RowStream({ resumeCount: 10, pauseCount: undefined! })
      ).toThrow('pauseCount must be provided with resumeCount');
    });

    it('requires pause count to be greater than 1', () => {
      for (const n of [-232, 0, 1]) {
        expect(() => new RowStream({ pauseCount: n })).toThrow(
          'pauseCount must more than 1'
        );
      }
    });

    it('automatically calculates resumeCount', () => {
      const rs = new RowStream({ pauseCount: 10 });
      expect(RowStream.stats(rs).resumeCount).toBe(5);
    });

    it('requires resumeCount to be less than pauseCount', () => {
      for (const rc of [20, 11, 10]) {
        expect(
          () => new RowStream({ pauseCount: 10, resumeCount: rc })
        ).toThrow('resumeCount must be less than pauseCount');
      }
    });

    it('resumeCount cannot be negative', () => {
      expect(() => new RowStream({ pauseCount: 10, resumeCount: -1 })).toThrow(
        'resumeCount cannot be negative'
      );
    });

    it('allows resumeCount to be 0', () => {
      const rs = new RowStream({ pauseCount: 10, resumeCount: 0 });
      expect(RowStream.stats(rs).resumeCount).toBe(0);
    });

    it('ignores empty options', () => {
      const rs = new RowStream(<RowStreamOptions>{});
      expect(RowStream.stats(rs)).toEqual({
        ready: false,
        size: 0,
        paused: false,
        canPause: false,
        pauseCount: undefined,
        resumeCount: undefined,
      });
    });
  });

  describe('next', () => {
    it('returns true and sets row if there is a row', async () => {
      const [rows, ctrl] = newRows();

      setCols(ctrl);
      ctrl.pushArray([2, 'malus', 'Apple']);

      const ok = await rows.next();
      expect(ok).toBe(true);
      expect(rows.row).toEqual([2, 'malus', 'Apple']);

      ctrl.end();
    });

    it('returns true and sets row if there is a row - already done', async () => {
      const [rows, ctrl] = newRows();

      setCols(ctrl);
      ctrl.pushArray([2, 'malus', 'Apple']);
      ctrl.end();

      const ok = await rows.next();
      expect(ok).toBe(true);
      expect(rows.row).toEqual([2, 'malus', 'Apple']);
    });

    it('resolves true and sets row when a row is received', async () => {
      const [rows, ctrl] = newRows();

      const pNext = rows.next();

      setCols(ctrl);
      ctrl.pushArray([2, 'malus', 'Apple']);

      const ok = await pNext;
      expect(ok).toBe(true);
      expect(rows.row).toEqual([2, 'malus', 'Apple']);

      ctrl.end();
    });

    it('emits resume if buffer goes back down to resumeCount', async () => {
      const [rows, ctrl] = newRows({
        pauseCount: 10,
        resumeCount: 6,
      });
      const msgs: string[] = [];
      ctrl.on('pause', () => msgs.push('pause'));
      ctrl.on('resume', () => msgs.push('resume'));

      setCols(ctrl);
      putData(ctrl, 10); // Add 10 rows

      expect(msgs).toEqual(['pause']);

      await rows.next(); // id: 1, cnt: 9
      await rows.next(); // id: 2, cnt: 8
      await rows.next(); // id: 3, cnt: 7

      // Not yet resumed
      expect(msgs).toEqual(['pause']);

      await rows.next(); // id: 4, cnt: 6

      // Now resumed
      expect(msgs).toEqual(['pause', 'resume']);
    });

    it('resolves false and closes rows if no data', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      const pNext = rows.next();
      ctrl.end();

      const ok = await pNext;

      expect(ok).toBe(false);

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('returns false if rows were already closed', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      ctrl.end();
      await rows.close();

      // Rows were already closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);

      const ok = await rows.next();

      expect(ok).toBe(false);
    });

    it('returns false and closes rows if no rows - already done', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      ctrl.end();

      // Should NOT be closed yet since next()
      // has never been called
      expect(RowStream.isClosed(rows)).toBe(false);

      const ok = await rows.next();
      expect(ok).toBe(false);

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('resolves false when rows closed itself resolves - close first', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      setCols(ctrl);
      ctrl.pushArray([2, 'malus', 'Apple']);

      rows.close();

      // rows.close itself will not resolve
      // until ctrl signals end
      setTimeout(() => ctrl.end(), 10);

      const result = await rows.next();
      expect(result).toBe(false);

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('resolves false when rows closed itself resolves - next first', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      const pNext = rows.next();

      rows.close();

      // rows.close itself will not resolve
      // until ctrl signals end
      setTimeout(() => ctrl.end(), 10);

      const result = await pNext;
      expect(result).toBe(false);

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('rejects if context was canceled', async () => {
      const [ctx, cancel] = Context.cancel();
      const [rows] = newRows(ctx.canceler);
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      const test = expect(rows.next()).rejects.toThrow('canceled');

      cancel();

      await test;

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('rejects on controller error', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      const test = expect(rows.next()).rejects.toThrow('on no');
      ctrl.error(new Error('on no'));

      await test;

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('rejects on null controller error', async () => {
      const [rows, ctrl] = newRows();
      let completeSignaled = false;
      rows.on('complete', () => (completeSignaled = true));

      const test = expect(rows.next()).rejects.toThrow(
        'Underlying stream encountered an error'
      );
      ctrl.error(null);

      await test;

      // Rows were automatically closed and signaled complete:
      expect(RowStream.isClosed(rows)).toBe(true);
      expect(completeSignaled).toBe(true);
    });

    it('rejects if another next call is still pending', async () => {
      const [rows] = newRows();
      rows.next();

      await expect(rows.next()).rejects.toThrow(
        'Existing next() call has not yet resolved'
      );
    });
  });

  describe('close', () => {
    it('does not resolve until controller ends - end', async () => {
      const [rows, ctrl] = newRows();

      const pClose = inspectPromise(rows.close());

      await wait(10);

      // Not yet resolved
      expect(pClose.done).toBe(false);

      ctrl.end();

      await wait(1);

      // Now should be resolved
      expect(pClose.resolved).toBe(true);
    });

    it('does not resolve until controller ends - err', async () => {
      const [rows, ctrl] = newRows();

      const pClose = inspectPromise(rows.close());

      await wait(10);

      // Not yet resolved
      expect(pClose.done).toBe(false);

      ctrl.error('bork');

      await wait(1);

      // Now should be resolved (not rejected)
      expect(pClose.resolved).toBe(true);
      expect(pClose.rejected).toBe(false);
    });

    it('returns if already closed', async () => {
      const [rows, ctrl] = newRows();
      ctrl.end();
      await rows.close();

      expect(rows.close()).resolves.toBe(undefined);
    });

    it('prevents further next() calls', async () => {
      const [rows, ctrl] = newRows();
      ctrl.end();
      await rows.close();

      const ok = await rows.next();
      expect(ok).toBe(false);
    });

    it('signals controller to cancel', async () => {
      const [rows, ctrl] = newRows();

      const msgs: string[] = [];
      ctrl.on('cancel', () => msgs.push('cancel'));

      const pClose = rows.close();

      expect(msgs).toEqual(['cancel']);

      ctrl.end();

      await expect(pClose).resolves.toBe(undefined);
    });

    it('clears cancellation callback', () => {
      const [ctx] = Context.cancel();
      const [rows] = newRows(ctx.canceler);

      expect(Canceler.size(ctx.canceler)).toBe(1);

      rows.close();

      expect(Canceler.size(ctx.canceler)).toBe(0);
    });
  });

  describe('row', () => {
    it('throws if next not yet called', () => {
      const [rows] = newRows();
      expect(() => rows.row).toThrow('No row loaded');
    });
  });

  describe('columns', () => {
    it('throws if column info not set', () => {
      const [rows] = newRows();
      expect(() => rows.columns).toThrow(
        'Column information not yet available'
      );
    });
  });

  describe('columnTypes', () => {
    it('throws if column info not set', () => {
      const [rows] = newRows();
      expect(() => rows.columnTypes).toThrow(
        'Column information not yet available'
      );
    });
  });

  describe('asyncIterator', () => {
    it('iterates and closes rows', async () => {
      const [rows, ctrl] = newRows();
      setCols(ctrl);

      // Adds some rows, but later
      later(() => putData(ctrl, 4), 10);

      // Signal end, later
      later(() => ctrl.end(), 15);

      const ids: number[] = [];
      for await (const row of rows) {
        ids.push(<number>row.id);
      }

      expect(ids).toEqual([1, 2, 3, 4]);
      expect(RowStream.isClosed(rows)).toBe(true);
    });

    it('closes rows if break from loop', async () => {
      const [rows, ctrl] = newRows();
      setCols(ctrl);

      // Adds some rows, but later
      later(() => putData(ctrl, 4), 10);

      // Signal end, later
      later(() => ctrl.end(), 15);

      const ids: number[] = [];
      for await (const row of rows) {
        ids.push(<number>row.id);
        if (ids.length > 2) break;
      }

      expect(ids).toEqual([1, 2, 3]);
      expect(RowStream.isClosed(rows)).toBe(true);
    });

    it('closes rows if throw from loop', async () => {
      const [rows, ctrl] = newRows();
      setCols(ctrl);

      // Adds some rows, but later
      later(() => putData(ctrl, 4), 10);

      // Signal end, later
      later(() => ctrl.end(), 15);

      const ids: number[] = [];
      try {
        for await (const row of rows) {
          ids.push(<number>row.id);
          if (ids.length > 2) {
            throw new Error('break!');
          }
        }
      } catch (e) {
        /* ignore */
      }

      expect(ids).toEqual([1, 2, 3]);
      expect(RowStream.isClosed(rows)).toBe(true);
    });
  });
});

describe('RowController', () => {
  describe('ready', () => {
    it('returns null if rows are already ready', async () => {
      const [, ctrl] = newRows();
      setCols(ctrl);

      await expect(ctrl.ready()).resolves.toBe(null);
    });

    it('returns null if rows already errored', async () => {
      const [, ctrl] = newRows();
      const err = new Error('fail');
      ctrl.error(err);

      await expect(ctrl.ready()).resolves.toBe(err);
    });

    it('resolves null when columns are provided', async () => {
      const [, ctrl] = newRows();
      const pReady = inspectPromise(ctrl.ready());

      await wait(1); // Next tick

      expect(pReady.done).toBe(false);

      setCols(ctrl);

      await wait(1); // Next tick

      expect(pReady.resolved).toBe(true);
    });

    it('resolves null when end is signalled', async () => {
      const [, ctrl] = newRows();
      const pReady = inspectPromise(ctrl.ready());

      await wait(1); // Next tick

      expect(pReady.done).toBe(false);

      ctrl.end();

      await wait(1); // Next tick

      expect(pReady.resolved).toBe(true);
    });

    it('resolves null when rows are closed', async () => {
      const [rows, ctrl] = newRows();
      const pReady = inspectPromise(ctrl.ready());

      await wait(1); // Next tick

      expect(pReady.done).toBe(false);

      rows.close();

      await wait(1); // Next tick

      expect(pReady.resolved).toBe(true);
      expect(pReady.result).toBe(null);
    });

    it('resolves err when err is signalled', async () => {
      const [, ctrl] = newRows();
      const pReady = inspectPromise(ctrl.ready());

      await wait(1); // Next tick

      expect(pReady.done).toBe(false);

      const err = new Error('the end');
      ctrl.error(err);

      await wait(1); // Next tick

      expect(pReady.resolved).toBe(true);
      expect(pReady.result).toBe(err);
    });

    it('resolves err when context is canceled', async () => {
      const [ctx, cancel] = Context.cancel();
      const [, ctrl] = newRows(ctx.canceler);
      const pReady = inspectPromise(ctrl.ready());

      await wait(1); // Next tick

      expect(pReady.done).toBe(false);

      cancel();

      await wait(1); // Next tick

      expect(pReady.resolved).toBe(true);
      expect(pReady.result).toBeInstanceOf(CanceledError);
    });
  });

  describe('setColumns', () => {
    it('sets column info', () => {
      const [rows, ctrl] = newRows();
      const colInfo = [
        { name: 'id', typeName: 'number', nullable: false },
        { name: 'code', typeName: 'string', nullable: false },
        { name: 'label', typeName: 'string', nullable: true },
      ];

      ctrl.setColumns(colInfo);

      expect(rows.columnTypes).toEqual(colInfo);
    });

    it('sets column names', () => {
      const [rows, ctrl] = newRows();
      const colInfo = [
        { name: 'id', typeName: 'number', nullable: false },
        { name: 'code', typeName: 'string', nullable: false },
        { name: 'label', typeName: 'string', nullable: true },
      ];

      ctrl.setColumns(colInfo);

      expect(rows.columns).toEqual(['id', 'code', 'label']);
    });
  });

  describe('pushRow', () => {
    const row = Row.fromObject(
      {
        id: 1,
        code: '1212',
        label: 'hello',
      },
      ['id', 'code', 'label']
    );

    it('adds a row to the buffer', async () => {
      const [rows, ctrl] = newRows();
      setCols(ctrl);

      ctrl.pushRow(row);

      expect(RowStream.size(rows)).toBe(1);
      await rows.next();

      expect(rows.row).toBe(row);
    });

    it('is ignored if rows are already canceling', () => {
      const [ctx, cancel] = Context.cancel();
      const [rows, ctrl] = newRows(ctx.canceler);

      setCols(ctrl);

      cancel();

      ctrl.pushRow(row);

      expect(RowStream.size(rows)).toBe(0);
    });

    it('throws if no columns set', () => {
      const [, ctrl] = newRows();
      expect(() => ctrl.pushRow(row)).toThrow('Column info not yet set');
    });
  });

  describe('pushArray', () => {
    it('creates a row using column info', async () => {
      const [rows, ctrl] = newRows();
      setCols(ctrl);
      ctrl.pushArray([1, 'abc', 'hello']);

      expect(RowStream.size(rows)).toBe(1);
      await rows.next();

      expect(Row.toObject(rows.row)).toEqual({
        id: 1,
        code: 'abc',
        label: 'hello',
      });
    });

    it('throws if no columns set', () => {
      const [, ctrl] = newRows();
      expect(() => ctrl.pushArray([1, 'abc', 'hello'])).toThrow(
        'Column info not yet set'
      );
    });
  });

  describe('pushObject', () => {
    it('creates a row using column info', async () => {
      const [rows, ctrl] = newRows();
      setCols(ctrl);
      ctrl.pushObject({
        id: 1,
        code: 'abc',
        label: 'hello',
      });

      expect(RowStream.size(rows)).toBe(1);
      await rows.next();

      expect(Row.toArray(rows.row)).toEqual([1, 'abc', 'hello']);
    });

    it('throws if no columns set', () => {
      const [, ctrl] = newRows();
      expect(() =>
        ctrl.pushObject({
          id: 1,
          code: 'abc',
          label: 'hello',
        })
      ).toThrow('Column info not yet set');
    });
  });

  describe('error', () => {
    it('sets the rows err property', () => {
      const [rows, ctrl] = newRows();
      const err = new Error('phooey');
      ctrl.error(err);

      expect(rows.err).toBe(err);
    });

    it('makes a default error', () => {
      const [rows, ctrl] = newRows();
      ctrl.error(null);

      expect(rows.err).toBeInstanceOf(Error);
      expect(rows.err?.message).toEqual(
        'Underlying stream encountered an error'
      );
    });

    it('wraps a string', () => {
      const [rows, ctrl] = newRows();
      ctrl.error('this is just terrible');

      expect(rows.err).toBeInstanceOf(Error);
      expect(rows.err?.message).toEqual('this is just terrible');
    });

    it('extracts a message', () => {
      const [rows, ctrl] = newRows();
      ctrl.error({ message: 'this is just terrible' });

      expect(rows.err).toBeInstanceOf(Error);
      expect(rows.err?.message).toEqual('this is just terrible');
    });

    it('stringifies anything else', () => {
      const [rows, ctrl] = newRows();
      ctrl.error(11);

      expect(rows.err).toBeInstanceOf(Error);
      expect(rows.err?.message).toEqual('11');
    });

    it('cancels the rows', () => {
      const [rows, ctrl] = newRows();
      ctrl.error(null);
      expect(RowStream.isClosed(rows)).toBe(true);
    });
  });

  describe('on', () => {
    it('registers pause handler', async () => {
      const [rows, ctrl] = newRows({
        pauseCount: 4,
        resumeCount: 2,
      });
      let pauseCnt = 0;
      ctrl.on('pause', () => pauseCnt++);

      setCols(ctrl);
      putData(ctrl, 4);

      expect(pauseCnt).toEqual(1);

      // Now drain;
      await rows.next();
      await rows.next();

      // Haven't paused again
      expect(pauseCnt).toEqual(1);

      // Now pause again
      putData(ctrl, 2);
      expect(pauseCnt).toEqual(2);
    });

    it('registers resume handler', async () => {
      const [rows, ctrl] = newRows({
        pauseCount: 2,
        resumeCount: 1,
      });
      let resumedCount = 0;
      ctrl.on('resume', () => resumedCount++);

      setCols(ctrl);
      putData(ctrl, 2);

      expect(resumedCount).toBe(0);

      await rows.next();

      expect(resumedCount).toBe(1);

      // Push to pause
      putData(ctrl, 1);
      expect(resumedCount).toBe(1);

      // Drain to resume
      await rows.next();

      expect(resumedCount).toBe(2);
    });

    it('registers cancel handler - rows close', () => {
      const [rows, ctrl] = newRows();
      let canceled = false;
      ctrl.on('cancel', () => (canceled = true));
      rows.close();
      expect(canceled).toBe(true);
    });

    it('registers cancel handler - controller err', () => {
      const [, ctrl] = newRows();
      let canceled = false;
      ctrl.on('cancel', () => (canceled = true));
      ctrl.error(null);
      expect(canceled).toBe(true);
    });

    it('registers cancel handler - context cancel', () => {
      const [ctx, cancel] = Context.cancel();
      const [, ctrl] = newRows(ctx.canceler);
      let canceled = false;
      ctrl.on('cancel', () => (canceled = true));

      cancel();

      expect(canceled).toBe(true);
    });
  });

  describe('off', () => {
    it('removes pause handler', async () => {
      const [rows, ctrl] = newRows({
        pauseCount: 4,
        resumeCount: 2,
      });
      let pauseCnt = 0;
      const onPause = () => pauseCnt++;
      ctrl.on('pause', onPause);

      setCols(ctrl);
      putData(ctrl, 4);

      expect(pauseCnt).toEqual(1);

      // Now drain;
      await rows.next();
      await rows.next();

      // Haven't paused again
      expect(pauseCnt).toEqual(1);

      ctrl.off('pause', onPause);

      // Now pause again
      putData(ctrl, 2);

      // But count still equal 1 because handler removed
      expect(pauseCnt).toEqual(1);
    });

    it('removes resume handler', async () => {
      const [rows, ctrl] = newRows({
        pauseCount: 2,
        resumeCount: 1,
      });
      let resumedCount = 0;
      const onResume = () => resumedCount++;
      ctrl.on('resume', onResume);

      setCols(ctrl);
      putData(ctrl, 2);

      expect(resumedCount).toBe(0);

      await rows.next();

      expect(resumedCount).toBe(1);

      // Push to pause
      putData(ctrl, 1);
      expect(resumedCount).toBe(1);

      ctrl.off('resume', onResume);

      // Drain to resume
      await rows.next();

      // Resume count still 1 because handler removed
      expect(resumedCount).toBe(1);
    });

    it('removes cancel handler', () => {
      const [ctx, cancel] = Context.cancel();
      const [, ctrl] = newRows(ctx.canceler);
      let canceled = false;

      const onCanceled = () => (canceled = true);
      ctrl.on('cancel', onCanceled);
      ctrl.off('cancel', onCanceled);

      cancel();

      // Still false because handler removed
      expect(canceled).toBe(false);
    });
  });

  describe('lifecycle test', () => {
    it('fills and reads concurrently', async () => {
      const [ctx, cancel] = Context.cancel();
      const [rows, ctrl] = newRows(ctx.canceler, {
        pauseCount: 6,
        resumeCount: 3,
      });

      // Side effect accumulators
      const msgs: string[] = [];
      const ids: number[] = [];

      const tl = new Timeline();

      const reset = () => {
        ids.splice(0, ids.length);
        msgs.splice(0, msgs.length);
      };

      const log = (msg: string) => {
        const readCnt = ids.length;
        const bufSize = RowStream.size(rows);
        msgs.push(tl.tick + ': ' + msg + ` - read ${readCnt}, buf ${bufSize}`);
      };

      const read = (row: Row) => ids.push(<number>row.id);

      // State tracking
      let paused = false;
      let canceled = false;

      ctrl.on('cancel', () => {
        canceled = true;
        log('stream canceled');
      });

      ctrl.on('pause', () => {
        paused = true;
        log('stream paused');
      });

      ctrl.on('resume', () => {
        paused = false;
        log('stream resumed');
        writeLoop();
      });

      // Input stream
      let id = 0;
      const writeLoop = () => {
        if (!canceled && !paused) {
          putData(ctrl, 1, id++);
          log('write');
          tl.setTimeout(writeLoop, 1);
        }
      };

      // Output stream
      const readLoop = async (n: number, pause: number) => {
        while (n > 0) {
          if (await rows.next()) {
            read(rows.row);
            log('read ');
          } else {
            log('done ');
            return;
          }

          n -= 1;
          if (n > 0 && pause > 0) {
            tl.setTimeout(() => readLoop(n, pause), pause);
            return;
          }
        }
      };

      // Schedule setCols for future
      tl.setTimeout(() => setCols(ctrl), 5);

      const pReady = ctrl.ready();

      tl.start();

      await tl.wait(1);

      // Not yet ready
      expect(RowStream.stats(rows).ready).toBe(false);

      // Can await
      await pReady;

      expect(tl.tick).toBe(5);

      // Start input
      reset();
      writeLoop();

      // Async read four rows as
      // fast as they are written
      // should take 4 ticks of writing
      readLoop(4, 1);

      await tl.wait(4);

      expect(tl.tick).toBe(9);

      // Read four records
      expect(ids).toEqual(range(4));

      // No messages yet
      expect(msgs).toEqual([
        '5: write - read 0, buf 1',
        '5: read  - read 1, buf 0',

        '6: write - read 1, buf 1',
        '6: read  - read 2, buf 0',

        '7: write - read 2, buf 1',
        '7: read  - read 3, buf 0',

        '8: write - read 3, buf 1',
        '8: read  - read 4, buf 0',

        '9: write - read 4, buf 1',
      ]);

      reset();

      // Now slowly read for a bit
      tl.setTimeout(() => readLoop(5, 4), 1);

      await tl.drain();

      expect(msgs).toEqual([
        '10: write - read 0, buf 2',
        '10: read  - read 1, buf 1',

        '11: write - read 1, buf 2',
        '12: write - read 1, buf 3',
        '13: write - read 1, buf 4',
        '14: write - read 1, buf 4', // 4 b/c next now resolved and dequeued a row
        '14: read  - read 2, buf 4',

        '15: write - read 2, buf 5',
        '16: stream paused - read 2, buf 6', // Pause occurs before write completes
        '16: write - read 2, buf 6',
        '18: read  - read 3, buf 5',
        '22: read  - read 4, buf 4',

        '26: stream resumed - read 4, buf 3', // Resume occurs before next resolves
        '26: write - read 4, buf 4', // Which immediately restarts write loop

        '26: read  - read 5, buf 4', // 4 b/c next now resolved and dequeued a row
        '27: write - read 5, buf 5',
        '28: stream paused - read 5, buf 6', // Pause occurs before write completes
        '28: write - read 5, buf 6',
      ]);

      expect(tl.tick).toBe(29);

      reset();

      tl.setTimeout(() => readLoop(6, 0), 1); // frame 30, also resumes ticking

      expect(tl.tick).toBe(30);

      tl.setTimeout(cancel, 2); // frame 32

      await tl.drain();

      expect(msgs).toEqual([
        '30: read  - read 1, buf 5',
        '30: read  - read 2, buf 4',
        '30: stream resumed - read 2, buf 3', // Resume occurs before next resolves
        '30: write - read 2, buf 4', // Which immediately restarts write loop
        '30: read  - read 3, buf 4',
        '30: read  - read 4, buf 3',
        '30: read  - read 5, buf 2',
        '30: read  - read 6, buf 1',
        '31: write - read 6, buf 2',
        '32: stream canceled - read 6, buf 2',
      ]);
    });
  });
});
