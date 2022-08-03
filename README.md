<!-- BEGIN:REMOVE_FOR_NPM -->
[![codecov](https://codecov.io/gh/libsabl/db-driver-js/branch/main/graph/badge.svg?token=TVL1XYSJHA)](https://app.codecov.io/gh/libsabl/db-driver-js/branch/main)
<span class="badge-npmversion"><a href="https://npmjs.org/package/@sabl/db-driver" title="View this project on NPM"><img src="https://img.shields.io/npm/v/@sabl/db-driver.svg" alt="NPM version" /></a></span>

<!-- END:REMOVE_FOR_NPM -->

# @sabl/db-driver

**db driver** provides common components for implementing database driver wrappers that meet the interfaces defined in [`@sabl/db-api`](https://github.com/libsabl/db-api-js). Authors can implement the core `DbPool`, `DbConn`, and `DbTxn` interfaces directly without this package, but the components here are useful in many situations. 
 
For more detail on the db api pattern, see sabl / [patterns](https://github.com/libsabl/patterns#patterns) / [db-api](https://github.com/libsabl/patterns/blob/main/patterns/db-api.md). 

<!-- BEGIN:REMOVE_FOR_NPM -->
> [**sabl**](https://github.com/libsabl/patterns) is an open-source project to identify, describe, and implement effective software patterns which solve small problems clearly, can be composed to solve big problems, and which work consistently across many programming languages.

## Developer orientation

See [SETUP.md](./docs/SETUP.md), [CONFIG.md](./docs/CONFIG.md).
<!-- END:REMOVE_FOR_NPM -->

## Summary

**Utility classes**
 
|Class|Description|Use Case|
|-|-|-|
|`RowStream`|A buffered stream that implements [`Rows`](https://github.com/libsabl/db-api-js#Rows). Clients can scroll through the concurrently-accumulating result set according the cursor API defined in `Rows`|Wrapping push-based APIs that emit rows through events or callbacks|
|`SqlTxn`|An implementation of `DbTxn` that uses standard SQL statements like `START TRANSACTION` and `COMMIT` to implement the transaction lifecycle|Wrapping platform APIs that do not have a native representation of a transaction|
|`DbQueue`|An implementation of `DbConn` that queues all calls to `exec`, `query`, `queryRow`, and `beginTxn` to ensure all actions occur in order on the underlying database connection, including ensuring that transactions are resolved before proceeding to subsequent queued operations.|Wrapping platform APIs that do not natively support async queuing of operations, or which do not provide a means of responding when the queue has been drained.|
|`DbPoolBase`|An implementation of `DbPool` that leverages `AsyncPool` from `@sabl/async` for concurrent-safe async pooling mechanics.|Simplifying the implementation of `DbPool`, even for platform APIs that have their own pool implementations.|

**Driver Interfaces**

In order to implement the common queuing algorithms in `DbQueue`, it must be possible to know when a transaction (`DbTxn`) has fully resolved, and when a row set (`Rows`) has been closed. The core db-api interfaces do not include a mechanism for this. 

This package defines several interfaces that add async completion-signalling to the underlying `Rows`, `DbTxn`, and `DbConn` types:

|Interface|Description|
|-|-|
|`CompleteEmitter`|Generic description of an object that supports subscribing to a `'complete'` via the standard `on` / `off` API as standardized in [EventEmitter](https://nodejs.org/api/events.html#class-eventemitter).|
|`DriverRows`|Composition of `Rows` with `CompleteEmitter`: A Rows that emits a `'complete'` event when it is closed.|
|`DriverTxn`|Composition of `Txn` with `CompleteEmitter`: A transaction which emits a `'complete'` event when it is closed, whether due to commit, rollback, or cancellation.|
|`DriverConn`|Augmentation of `DbConn` that returns a `DriverRows` from `query`, and a `DriverTxn` from `beginTxn`. Allows `DbQueue` to know when it can proceed to the next operation, and likewise allows `DbQueue` to signal when its work is complete (or cancelled) and its underlying `DriverConn` can be returned to the pool in `DbPoolBase`|

Note that `SqlTxn` supports `DriverTxn`, and `RowStream` supports `DriverRows`, so authors can implement the full `DbApi` interface set with a fairly concise set of wrappers that use all four of the utility classes in this package. The test fixtures in this package include a complete implementation of a wrapper for `sqlite3`.