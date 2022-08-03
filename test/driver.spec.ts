// Copyright 2022 Joshua Honig. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

import { parseCols } from '$';
import { ColumnInfo } from '@sabl/db-api';

describe('parseCols', () => {
  it('makes an array of cols', () => {
    const cols = parseCols({
      stringCol: 'hello',
      numCol: 2,
      boolCol1: true,
      boolCol2: false,
      binCol: Uint8Array.from([1, 2, 3]),
      dateCol: new Date(),
    });

    const expected: ColumnInfo[] = [
      {
        name: 'stringCol',
        typeName: 'string',
        nullable: false,
      },
      {
        name: 'numCol',
        typeName: 'number',
        nullable: false,
      },
      {
        name: 'boolCol1',
        typeName: 'boolean',
        nullable: false,
      },
      {
        name: 'boolCol2',
        typeName: 'boolean',
        nullable: false,
      },
      {
        name: 'binCol',
        typeName: 'binary',
        nullable: false,
      },
      {
        name: 'dateCol',
        typeName: 'datetime',
        nullable: false,
      },
    ];

    expect(cols).toEqual(expected);
  });

  it('parses null prop as nullable unknown', () => {
    const cols = parseCols({
      stringCol: 'hello',
      emptyCol1: null,
      emptyCol2: undefined,
    });

    const expected: ColumnInfo[] = [
      {
        name: 'stringCol',
        typeName: 'string',
        nullable: false,
      },
      {
        name: 'emptyCol1',
        typeName: 'unknown',
        nullable: true,
      },
      {
        name: 'emptyCol2',
        typeName: 'unknown',
        nullable: true,
      },
    ];

    expect(cols).toEqual(expected);
  });
});
