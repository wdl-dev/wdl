export const consoleFormatting = {
  test() {
    for (const method of ["log", "error"]) {
      console[method]({
        get [Symbol.toStringTag]() {
          throw Object.create(null);
        },
      });
    }
  },
};

export const shortLivedWrappers = {
  test() {
    // Exceed the pre-fix native pointer-table capacity without retaining objects
    // or forcing GC: allocation must itself trigger reclamation.
    let encoding;
    for (let i = 0; i < 40_000_000; i++) encoding = new TextEncoder().encoding;
    if (encoding !== "utf-8") throw new Error("unexpected TextEncoder encoding");
  },
};
