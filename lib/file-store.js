const fs = require("fs");
const path = require("path");
const { nowIso, sanitizeFileName } = require("./utils");

function ensureDirectory(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function writeJsonAtomic(filePath, value) {
  ensureDirectory(path.dirname(filePath));

  const tmpPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmpPath, JSON.stringify(value, null, 2), "utf8");
  fs.renameSync(tmpPath, filePath);
}

function readJsonFile(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") {
      return fallback;
    }

    throw error;
  }
}

class EventStore {
  constructor(baseDir = path.join(process.cwd(), "data")) {
    this.baseDir = baseDir;
    this.eventsDir = path.join(baseDir, "events");
    ensureDirectory(this.eventsDir);
  }

  getEventPath(eventId) {
    return path.join(this.eventsDir, `${sanitizeFileName(eventId)}.json`);
  }

  getEvent(eventId) {
    return readJsonFile(this.getEventPath(eventId), null);
  }

  upsertEvent(eventId, patch) {
    const current = this.getEvent(eventId) || {
      eventId,
      createdAt: nowIso(),
      history: []
    };
    const history = Array.isArray(current.history) ? current.history.slice() : [];

    if (patch.status) {
      history.push({
        status: patch.status,
        at: nowIso(),
        note: patch.note || null
      });
    }

    const nextValue = {
      ...current,
      ...patch,
      history,
      updatedAt: nowIso()
    };

    writeJsonAtomic(this.getEventPath(eventId), nextValue);
    return nextValue;
  }

  listEvents() {
    const fileNames = fs.readdirSync(this.eventsDir);

    return fileNames
      .filter((fileName) => fileName.endsWith(".json"))
      .map((fileName) => readJsonFile(path.join(this.eventsDir, fileName), null))
      .filter(Boolean);
  }

  findRecentEvents({ userId, message, withinHours = 24, excludeEventId = null }) {
    const now = Date.now();
    const windowStart = now - withinHours * 60 * 60 * 1000;

    return this.listEvents().filter((event) => {
      if (!event) {
        return false;
      }

      if (excludeEventId && event.eventId === excludeEventId) {
        return false;
      }

      if (userId && event.fromUserId !== userId) {
        return false;
      }

      if (message && event.normalizedMessage !== message) {
        return false;
      }

      const createdTime = Date.parse(event.createdAt || event.receivedAt || event.updatedAt || "");
      return Number.isFinite(createdTime) && createdTime >= windowStart;
    });
  }
}

class BlacklistStore {
  constructor(baseDir = path.join(process.cwd(), "data")) {
    this.filePath = path.join(baseDir, "blacklist.json");
    ensureDirectory(baseDir);
  }

  read() {
    return readJsonFile(this.filePath, {});
  }

  isBlacklisted(userId) {
    if (!userId) {
      return false;
    }

    const blacklist = this.read();
    return Boolean(blacklist[userId]);
  }

  add(userId, payload) {
    if (!userId) {
      return null;
    }

    const blacklist = this.read();
    const current = blacklist[userId] || {
      firstSeenAt: nowIso(),
      hitCount: 0
    };

    blacklist[userId] = {
      ...current,
      ...payload,
      hitCount: (current.hitCount || 0) + 1,
      updatedAt: nowIso()
    };

    writeJsonAtomic(this.filePath, blacklist);
    return blacklist[userId];
  }
}

module.exports = {
  EventStore,
  BlacklistStore
};
