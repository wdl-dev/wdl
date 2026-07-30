import { stripR2PhysicalPrefixWith } from "runtime-r2-utils";
import { collectXmlFields, xmlEscape, xmlLocalName, xmlUnescape } from "shared-s3-xml";
import { stripEtag } from "runtime-bindings-r2-metadata";

export { xmlEscape, xmlUnescape };

const LIST_CONTENT_TAGS = new Set(["Key", "Size", "ETag", "LastModified"]);
const LIST_PREFIX_TAGS = new Set(["Prefix"]);

/**
 * @param {string} xml
 * @param {string} physicalPrefix
 */
export function parseListObjects(xml, physicalPrefix) {
  const objects = [];
  const delimitedPrefixes = [];
  let cursor;
  let truncated = false;

  const entryRe = /<((?:[A-Za-z_][A-Za-z0-9_.-]*:)?(?:Contents|CommonPrefixes))>([\s\S]*?)<\/\1>|<((?:[A-Za-z_][A-Za-z0-9_.-]*:)?IsTruncated)>([\s\S]*?)<\/\3>|<((?:[A-Za-z_][A-Za-z0-9_.-]*:)?NextContinuationToken)>([\s\S]*?)<\/\5>/g;
  for (const match of xml.matchAll(entryRe)) {
    const entry = xmlLocalName(match[1] || match[3] || match[5]);
    if (entry === "Contents") {
      const fields = collectXmlFields(match[2], LIST_CONTENT_TAGS);
      if (!fields.Key) continue;
      const httpEtag = fields.ETag || "";
      const uploaded = fields.LastModified ? new Date(fields.LastModified).getTime() : Date.now();
      objects.push({
        key: stripR2PhysicalPrefixWith(physicalPrefix, fields.Key),
        version: "",
        size: Number(fields.Size ?? "0"),
        etag: stripEtag(httpEtag),
        httpEtag,
        uploaded: Number.isFinite(uploaded) ? uploaded : Date.now(),
        httpMetadata: {},
        customMetadata: {},
        checksums: {},
        storageClass: "Standard",
      });
    } else if (entry === "CommonPrefixes") {
      const fields = collectXmlFields(match[2], LIST_PREFIX_TAGS);
      if (fields.Prefix) {
        delimitedPrefixes.push(stripR2PhysicalPrefixWith(physicalPrefix, fields.Prefix));
      }
    } else if (entry === "IsTruncated") {
      truncated = xmlUnescape(match[4]).trim() === "true";
    } else if (entry === "NextContinuationToken") {
      cursor = xmlUnescape(match[6]);
    }
  }
  return {
    objects,
    truncated,
    ...(cursor ? { cursor } : {}),
    delimitedPrefixes,
  };
}
