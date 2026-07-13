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
 * Temporarily replaces an accessor or other descriptor for one async scope.
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
  const originalDescriptor = Object.getOwnPropertyDescriptor(target, name);
  Object.defineProperty(target, name, descriptor);
  try {
    return /** @type {Awaited<ReturnType<TCallback>>} */ (await callback());
  } finally {
    if (originalDescriptor) Object.defineProperty(target, name, originalDescriptor);
    else delete target[name];
  }
}
