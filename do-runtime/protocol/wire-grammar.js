export const MAX_ID_BYTES = 512;
export const CLASS_NAME_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
export const METHOD_NAME_RE = /^[A-Za-z_$][A-Za-z0-9_$]*$/;
export const HEADER_NAME_RE = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
export const STORAGE_ID_RE = /^[a-z0-9_-]+$/;
export const HOST_ID_RE = /^[a-z0-9_-]+:[A-Za-z_$][A-Za-z0-9_$]*:shard(?:0|[1-9][0-9]*)$/;
export const DO_HOST_SHARD_COUNT = 16;
