const fs = require("fs");
const path = require("path");
const { nowIso } = require("./utils");

function ensureDirectory(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function readJson(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return fallback;
    }
    throw error;
  }
}

function writeJsonAtomic(filePath, value) {
  ensureDirectory(path.dirname(filePath));
  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(value, null, 2), "utf8");
  fs.renameSync(tmpPath, filePath);
}

class FileIdempotencyStore {
  constructor(baseDir = path.join(process.cwd(), "data")) {
    this.filePath = path.join(baseDir, "idempotency-keys.json");
    ensureDirectory(baseDir);
  }

  async isProcessed(commandId) {
    if (!commandId) {
      return false;
    }

    const data = readJson(this.filePath, {});
    return Boolean(data[commandId] && data[commandId].status === "success");
  }

  async get(commandId) {
    const data = readJson(this.filePath, {});
    return data[commandId] || null;
  }

  async markProcessed(commandId, payload = {}) {
    const data = readJson(this.filePath, {});
    data[commandId] = {
      commandId,
      status: "success",
      processedAt: nowIso(),
      ...payload
    };
    writeJsonAtomic(this.filePath, data);
    return data[commandId];
  }
}

module.exports = {
  FileIdempotencyStore
};
