interface WdlNodeBuffer extends Uint8Array {
  toString(encoding?: "base64" | "utf8", start?: number, end?: number): string;
  indexOf(searchElement: number, fromIndex?: number): number;
  indexOf(value: Uint8Array, byteOffset?: number): number;
}

interface WdlNodeBufferConstructor {
  alloc(size: number): WdlNodeBuffer;
  from(value: string, encoding?: "base64" | "utf8"): WdlNodeBuffer;
  from(value: Uint8Array | ArrayLike<number>): WdlNodeBuffer;
  from(arrayBuffer: ArrayBufferLike, byteOffset?: number, length?: number): WdlNodeBuffer;
  byteLength(value: string, encoding?: "utf8"): number;
}

interface WdlNodeProcess {
  env?: Record<string, string | undefined>;
}
