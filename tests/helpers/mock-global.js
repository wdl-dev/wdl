const IntrinsicAggregateError = AggregateError;
const intrinsicObjectCreate = Object.create;
const intrinsicObjectDefineProperty = Object.defineProperty;
const intrinsicObjectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const intrinsicReflectDeleteProperty = Reflect.deleteProperty;
/** @type {typeof Symbol.iterator} */
const intrinsicSymbolIterator = Symbol.iterator;

/**
 * Replaces one global property and returns an idempotent restore function.
 *
 * Use this for tests that need temporary global API mocks. Prefer
 * withMockedGlobal when the mock has one lexical async scope.
 *
 * @template {keyof typeof globalThis} K
 * @param {K} name
 * @param {(typeof globalThis)[K]} mockImpl
 * @returns {() => void}
 */
export function installMockGlobal(name, mockImpl) {
  return installMockProperty(globalThis, name, mockImpl);
}

/**
 * Replaces one object property and returns an idempotent restore function.
 *
 * Use this for globals with property-level APIs such as `console.log`, where
 * replacing the whole global object would be broader than the test needs.
 *
 * @template {object} T
 * @template {keyof T} K
 * @param {T} target
 * @param {K} name
 * @param {T[K]} mockImpl
 * @returns {() => void}
 */
export function installMockProperty(target, name, mockImpl) {
  const originalDescriptor = Object.getOwnPropertyDescriptor(target, name);
  target[name] = mockImpl;
  let restored = false;
  return () => {
    if (restored) return;
    if (target[name] !== mockImpl) {
      throw new Error(`Cannot restore mocked property ${String(name)} out of order`);
    }
    if (originalDescriptor) {
      Object.defineProperty(target, name, originalDescriptor);
    } else {
      delete target[name];
    }
    restored = true;
  };
}

/**
 * Temporarily replaces one global property for a single async test scope.
 *
 * Use this instead of hand-written save/restore blocks so failed assertions do
 * not leak a mocked global into later tests.
 *
 * @template {keyof typeof globalThis} K
 * @template {() => unknown | Promise<unknown>} TCallback
 * @param {K} name
 * @param {(typeof globalThis)[K]} mockImpl
 * @param {TCallback} callback
 * @returns {Promise<Awaited<ReturnType<TCallback>>>}
 */
export async function withMockedGlobal(name, mockImpl, callback) {
  return await withMockedProperty(globalThis, name, mockImpl, callback);
}

/**
 * Temporarily replaces one object property for a single async test scope.
 *
 * @template {object} T
 * @template {keyof T} K
 * @template {() => unknown | Promise<unknown>} TCallback
 * @param {T} target
 * @param {K} name
 * @param {T[K]} mockImpl
 * @param {TCallback} callback
 * @returns {Promise<Awaited<ReturnType<TCallback>>>}
 */
export async function withMockedProperty(target, name, mockImpl, callback) {
  const restore = installMockProperty(target, name, mockImpl);
  try {
    return /** @type {Awaited<ReturnType<TCallback>>} */ (await callback());
  } finally {
    restore();
  }
}

/**
 * @param {object} target
 * @param {PropertyKey} name
 * @param {PropertyDescriptor | undefined} descriptor
 */
function restorePropertyDescriptor(target, name, descriptor) {
  if (descriptor) {
    intrinsicObjectDefineProperty(target, name, descriptor);
    return;
  }
  if (!intrinsicReflectDeleteProperty(target, name)) {
    throw new Error(`Cannot restore mocked property ${String(name)}`);
  }
}

/**
 * Exposes prototype-free indexed bookkeeping as an iterable without relying on
 * Array.prototype, which may itself be mocked by the callback.
 *
 * @param {Record<number, unknown>} values
 * @param {number} count
 * @returns {Iterable<unknown>}
 */
function indexedValues(values, count) {
  return /** @type {Iterable<unknown>} */ ({
    [intrinsicSymbolIterator]() {
      let index = 0;
      return {
        next() {
          if (index >= count) return { done: true, value: undefined };
          const value = values[index];
          index += 1;
          return { done: false, value };
        },
      };
    },
  });
}

/**
 * Temporarily replaces an accessor or other descriptor for one async scope.
 * The temporary descriptor is installed as configurable to support cleanup.
 *
 * @template {object} T
 * @template {keyof T} K
 * @template {() => unknown | Promise<unknown>} TCallback
 * @param {T} target
 * @param {K} name
 * @param {PropertyDescriptor} descriptor
 * @param {TCallback} callback
 * @returns {Promise<Awaited<ReturnType<TCallback>>>}
 */
export async function withMockedPropertyDescriptor(target, name, descriptor, callback) {
  return await withMockedPropertyDescriptors([
    { target, name, descriptor },
  ], callback);
}

/**
 * Temporarily replaces several descriptors for one async test scope and
 * restores them in reverse installation order. Restoration is best-effort if
 * the callback makes a temporary descriptor non-configurable.
 *
 * @template {() => unknown | Promise<unknown>} TCallback
 * @param {Array<{ target: object, name: PropertyKey, descriptor: PropertyDescriptor }>} mocks
 * @param {TCallback} callback
 * @returns {Promise<Awaited<ReturnType<TCallback>>>}
 */
export async function withMockedPropertyDescriptors(mocks, callback) {
  // Mocks may replace Array.prototype and descriptor APIs, so use captured
  // intrinsics plus prototype-free restoration bookkeeping.
  /** @type {Record<number, { target: object, name: PropertyKey, descriptor: PropertyDescriptor | undefined }>} */
  const originals = intrinsicObjectCreate(null);
  let originalCount = 0;
  /** @type {Record<number, unknown>} */
  const failures = intrinsicObjectCreate(null);
  let failureCount = 0;
  let scopeFailed = false;
  let result;
  try {
    for (let i = 0; i < mocks.length; i += 1) {
      const { target, name, descriptor } = mocks[i];
      const originalDescriptor = intrinsicObjectGetOwnPropertyDescriptor(target, name);
      if (originalDescriptor && !originalDescriptor.configurable) {
        throw new TypeError(
          `Cannot temporarily mock non-configurable property ${String(name)}`
        );
      }
      originals[originalCount] = {
        target,
        name,
        descriptor: originalDescriptor,
      };
      originalCount += 1;
      intrinsicObjectDefineProperty(target, name, {
        ...descriptor,
        configurable: true,
      });
    }
    result = await callback();
  } catch (error) {
    scopeFailed = true;
    failures[failureCount] = error;
    failureCount += 1;
  }

  const restorationFailureOffset = failureCount;
  for (let i = originalCount - 1; i >= 0; i -= 1) {
    try {
      const { target, name, descriptor } = originals[i];
      restorePropertyDescriptor(target, name, descriptor);
    } catch (error) {
      failures[failureCount] = error;
      failureCount += 1;
    }
  }

  if (failureCount > restorationFailureOffset) {
    throw new IntrinsicAggregateError(
      indexedValues(failures, failureCount),
      scopeFailed
        ? "Mock scope failed and descriptor restoration also failed"
        : "Failed to restore mocked property descriptors"
    );
  }
  if (scopeFailed) throw failures[0];
  return /** @type {Awaited<ReturnType<TCallback>>} */ (result);
}
