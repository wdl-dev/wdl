export {};

type WdlIsAny<T> = 0 extends (1 & T) ? true : false;
type WdlAssertFalse<T extends false> = T;

// Keep Workers ambient declarations out of the Node-only checker. The current
// package widens Buffer to any and conflicts with Node's global shape.
type WdlNodeBufferMustRemainTyped = WdlAssertFalse<WdlIsAny<typeof Buffer>>;

// Bare process currently still resolves as NodeJS.Process when the declarations
// are mixed; retain this assertion against a future direct widening.
type WdlNodeProcessMustRemainTyped = WdlAssertFalse<WdlIsAny<typeof process>>;

// Current Workers pollution removes process from the Node global shape instead
// of merely widening it. Property access and the any assertion guard both forms.
type WdlNodeGlobalMustExposeTypedProcess = WdlAssertFalse<WdlIsAny<typeof global.process>>;
