// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { promise } from '@sabl/async';
import { inspectPromise } from './fixtures/utils';

it('starts unresolved', () => {
  const base = promise<boolean>();
  const wrapped = inspectPromise(base);

  expect(wrapped.done).toBe(false);
  expect(wrapped.resolved).toBe(false);
  expect(wrapped.rejected).toBe(false);
  expect(wrapped.result).toBe(undefined);
  expect(wrapped.reason).toBe(undefined);

  base.resolve(true);
});

it('sets resolved, done, and result', async () => {
  const base = promise<number>();
  const wrapped = inspectPromise(base);
  base.resolve(11);

  // Need to await to allow promise
  // internals to resolve.
  const result = await wrapped;

  expect(wrapped.done).toBe(true);
  expect(wrapped.resolved).toBe(true);
  expect(wrapped.rejected).toBe(false);
  expect(wrapped.result).toBe(11);
  expect(wrapped.reason).toBe(undefined);

  expect(result).toBe(11);
});

it('sets rejected, done, and reason', async () => {
  const base = promise<number>();
  const wrapped = inspectPromise(base);
  const err = new Error('oh no!');
  base.reject(err);

  // Need to await to allow promise
  // internals to resolve.
  await expect(wrapped).rejects.toThrow(err);

  expect(wrapped.done).toBe(true);
  expect(wrapped.resolved).toBe(false);
  expect(wrapped.rejected).toBe(true);
  expect(wrapped.result).toBe(undefined);
  expect(wrapped.reason).toBe(err);
});
