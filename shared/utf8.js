const SHORT_STRING_MAX_CODE_UNITS = 512;
const utf8Encoder = new TextEncoder();

/**
 * Return the UTF-8 byte length without allocating for ordinary short strings.
 * TextEncoder owns the long-string path where the native implementation wins.
 *
 * @param {string} value
 * @returns {number}
 */
export function utf8ByteLength(value) {
  if (value.length > SHORT_STRING_MAX_CODE_UNITS) {
    return utf8Encoder.encode(value).byteLength;
  }
  let bytes = 0;
  for (let i = 0; i < value.length; i += 1) {
    const code = value.charCodeAt(i);
    if (code < 0x80) {
      bytes += 1;
    } else if (code < 0x800) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        i += 1;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
}
