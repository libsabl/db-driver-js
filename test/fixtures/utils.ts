// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

/**
 * A decorated promise which exposes synchronous
 * properties to examine the state of the promise
 */
export interface InspectablePromise<T> extends Promise<T> {
  /** Equivalent to resolved || rejected */
  readonly done: boolean;

  /** True if the promise has resolved */
  readonly resolved: boolean;

  /** True if the promise was rejected */
  readonly rejected: boolean;

  /** The value if the promise was resolved */
  readonly result?: T;

  /** The reason if the promise was rejected */
  readonly reason?: unknown;
}

export function inspectPromise<T>(promise: Promise<T>): InspectablePromise<T> {
  let done = false;
  let resolved = false;
  let rejected = false;
  let _result: T | undefined = undefined;
  let _reason: unknown = undefined;

  const wrapped = promise
    .finally(() => {
      done = true;
    })
    .then((value) => {
      resolved = true;
      _result = value;
      return value;
    })
    .catch((reason) => {
      rejected = true;
      _reason = reason;
      throw reason;
    });

  Object.defineProperties(wrapped, {
    done: {
      get() {
        return done;
      },
    },
    resolved: {
      get() {
        return resolved;
      },
    },
    rejected: {
      get() {
        return rejected;
      },
    },
    result: {
      get() {
        return _result;
      },
    },
    reason: {
      get() {
        return _reason;
      },
    },
  });

  return <InspectablePromise<T>>wrapped;
}
