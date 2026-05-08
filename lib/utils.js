const crypto = require("crypto");

function nowIso() {
  return new Date().toISOString();
}

function stableHash(value) {
  const serialized = typeof value === "string" ? value : JSON.stringify(value);
  return crypto.createHash("sha256").update(serialized).digest("hex");
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFKC")
    .replace(/\s+/g, " ")
    .trim();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractJsonObject(text) {
  const input = String(text || "").trim();
  const fenced = input.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const candidate = fenced ? fenced[1].trim() : input;

  try {
    return JSON.parse(candidate);
  } catch (error) {
    const firstBrace = candidate.indexOf("{");
    const lastBrace = candidate.lastIndexOf("}");

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      return JSON.parse(candidate.slice(firstBrace, lastBrace + 1));
    }

    throw error;
  }
}

function sanitizeFileName(value) {
  return encodeURIComponent(String(value || "unknown"));
}

function createRetryableError(message, details = {}) {
  const error = new Error(message);
  error.retryable = true;
  Object.assign(error, details);
  return error;
}

function isRetryableError(error) {
  if (!error) {
    return false;
  }

  if (error.retryable) {
    return true;
  }

  if (error.retriable) {
    return true;
  }

  const status = error.response && error.response.status;

  if (status === 408 || status === 409 || status === 425 || status === 429) {
    return true;
  }

  if (status >= 500) {
    return true;
  }

  return ["ECONNABORTED", "ECONNRESET", "ETIMEDOUT", "ENOTFOUND"].includes(error.code);
}

module.exports = {
  nowIso,
  stableHash,
  normalizeText,
  sleep,
  extractJsonObject,
  sanitizeFileName,
  createRetryableError,
  isRetryableError
};
