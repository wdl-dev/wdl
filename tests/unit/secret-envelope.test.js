import { test } from "node:test";
import assert from "node:assert/strict";

import {
  SECRET_ENVELOPE_PREFIX,
  bytesToBase64,
  decryptSecretValue,
  encryptSecretValue,
  isSecretEnvelope,
} from "../../shared/secret-envelope.js";
import { readRepositoryJson } from "../helpers/load-shared-module.js";
import { withMockedProperty } from "../helpers/mock-global.js";

const SECRET_ENVELOPE_PARITY = readRepositoryJson("tests/fixtures/secret-envelope-parity.json");
const env = {
  SECRET_ENVELOPE_LOCAL_KEY_B64: SECRET_ENVELOPE_PARITY.provider.localKeyB64,
  SECRET_ENVELOPE_KID: SECRET_ENVELOPE_PARITY.provider.kid,
};

function deterministicRandomFactory(start = 0) {
  let next = start;
  return (/** @type {number} */ length) => {
    const out = new Uint8Array(length);
    for (let i = 0; i < length; i++) out[i] = next++ & 0xff;
    return out;
  };
}

test("secret envelope generation and decrypt match the shared parity vectors", async () => {
  for (const vector of SECRET_ENVELOPE_PARITY.vectors) {
    const envelope = await encryptSecretValue(vector.plaintext, {
      env,
      hashKey: vector.hashKey,
      fieldName: vector.fieldName,
      random: deterministicRandomFactory(vector.randomStart),
    });

    assert.equal(envelope, vector.envelope, vector.name);
    assert.equal(isSecretEnvelope(envelope), true, vector.name);
    if (vector.plaintext !== "") assert.equal(envelope.includes(vector.plaintext), false, vector.name);
    assert.equal(
      await decryptSecretValue(envelope, {
        env,
        hashKey: vector.hashKey,
        fieldName: vector.fieldName,
      }),
      vector.plaintext,
      vector.name
    );
  }
});

test("secret envelope reuses the configured KEK while importing each DEK independently", async () => {
  const firstKey = new Uint8Array(32).fill(0xa1);
  const secondKey = new Uint8Array(32).fill(0xb2);
  const firstEnv = {
    SECRET_ENVELOPE_LOCAL_KEY_B64: bytesToBase64(firstKey),
    SECRET_ENVELOPE_KID: "local:test:secret-envelope:cache",
  };
  const secondEnv = {
    ...firstEnv,
    SECRET_ENVELOPE_LOCAL_KEY_B64: bytesToBase64(secondKey),
  };
  /** @type {Uint8Array[]} */
  const importedKeys = [];
  const importKey = new Proxy(crypto.subtle.importKey, {
    apply(target, thisArg, args) {
      assert.ok(args[1] instanceof Uint8Array);
      importedKeys.push(args[1].slice());
      return Reflect.apply(target, thisArg, args);
    },
  });

  await withMockedProperty(crypto.subtle, "importKey", importKey, async () => {
    const firstEnvelope = await encryptSecretValue("first", {
      env: firstEnv,
      hashKey: "secrets:cache",
      fieldName: "FIRST",
      random: deterministicRandomFactory(),
    });
    await encryptSecretValue("second", {
      env: firstEnv,
      hashKey: "secrets:cache",
      fieldName: "SECOND",
      random: deterministicRandomFactory(64),
    });
    assert.equal(await decryptSecretValue(firstEnvelope, {
      env: firstEnv,
      hashKey: "secrets:cache",
      fieldName: "FIRST",
    }), "first");
    await encryptSecretValue("rotated", {
      env: secondEnv,
      hashKey: "secrets:cache",
      fieldName: "ROTATED",
      random: deterministicRandomFactory(128),
    });
  });

  assert.equal(importedKeys.filter((key) => bytesToBase64(key) === bytesToBase64(firstKey)).length, 1);
  assert.equal(importedKeys.filter((key) => bytesToBase64(key) === bytesToBase64(secondKey)).length, 1);
  assert.equal(importedKeys.length, 6);
});

test("secret envelope rejection behavior matches the shared parity vectors", async () => {
  for (const rejection of SECRET_ENVELOPE_PARITY.rejections) {
    await assert.rejects(
      decryptSecretValue(rejection.envelope, {
        env: { ...env, SECRET_ENVELOPE_KID: rejection.configuredKid },
        hashKey: rejection.hashKey,
        fieldName: rejection.fieldName,
      }),
      { code: rejection.jsErrorCode },
      rejection.name
    );
  }
});

test("secret envelope requires explicit local provider configuration", async () => {
  await assert.rejects(
    encryptSecretValue("value", {
      env: {},
      hashKey: "secrets:demo",
      fieldName: "TOKEN",
    }),
    { code: "secret_encryption_unconfigured" }
  );
});

test("secret envelope rejects unprefixed values", async () => {
  await assert.rejects(
    decryptSecretValue("plain-secret", {
      env,
      hashKey: "secrets:demo",
      fieldName: "TOKEN",
    }),
    { code: "secret_not_encrypted" }
  );
});

test("secret envelope reports invalid local provider key as configuration error", async () => {
  await assert.rejects(
    encryptSecretValue("value", {
      env: {
        SECRET_ENVELOPE_LOCAL_KEY_B64: "not-base64",
        SECRET_ENVELOPE_KID: "local:test:secret-envelope:v1",
      },
      hashKey: "secrets:demo",
      fieldName: "TOKEN",
    }),
    { code: "secret_encryption_unconfigured" }
  );
});

test("secret envelope rejects unknown JSON fields like redis-proxy", async () => {
  const envelope = await encryptSecretValue("value", {
    env,
    hashKey: "secrets:demo",
    fieldName: "TOKEN",
    random: deterministicRandomFactory(),
  });
  const parsed = JSON.parse(envelope.slice(SECRET_ENVELOPE_PREFIX.length));
  const withExtra = `${SECRET_ENVELOPE_PREFIX}${JSON.stringify({ ...parsed, extra: "ignored?" })}`;

  await assert.rejects(
    decryptSecretValue(withExtra, {
      env,
      hashKey: "secrets:demo",
      fieldName: "TOKEN",
    }),
    { code: "invalid_envelope" }
  );
});

test("secret envelope rejects duplicate JSON fields like redis-proxy", async () => {
  const envelope = await encryptSecretValue("value", {
    env,
    hashKey: "secrets:demo",
    fieldName: "TOKEN",
    random: deterministicRandomFactory(),
  });
  const json = envelope.slice(SECRET_ENVELOPE_PREFIX.length);
  const duplicated = `${SECRET_ENVELOPE_PREFIX}${json.replace("\"v\":1", "\"v\":999,\"v\":1")}`;

  await assert.rejects(
    decryptSecretValue(duplicated, {
      env,
      hashKey: "secrets:demo",
      fieldName: "TOKEN",
    }),
    { code: "invalid_envelope" }
  );
});
