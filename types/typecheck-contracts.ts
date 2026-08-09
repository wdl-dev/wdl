export {};

type WdlIsAny<T> = 0 extends (1 & T) ? true : false;
type WdlAssertFalse<T extends false> = T;

// Workers declarations share one checker with @types/node. A Workers release must not
// erase Node's Buffer contract by redeclaring the global as `any`.
type WdlNodeBufferMustRemainTyped = WdlAssertFalse<WdlIsAny<typeof Buffer>>;
type WdlNodeProcessMustRemainTyped = WdlAssertFalse<WdlIsAny<typeof process>>;
type WdlNodeGlobalProcessMustRemainTyped = WdlAssertFalse<WdlIsAny<typeof global.process>>;
