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

function parseEventTime(value) {
  if (!value) {
    return NaN;
  }

  if (typeof value === "number") {
    // Facebook sometimes sends Unix seconds, while Messenger can send milliseconds.
    return value < 100000000000 ? value * 1000 : value;
  }

  const text = String(value).trim();

  if (/^\d+$/.test(text)) {
    const numberValue = Number(text);
    return numberValue < 100000000000 ? numberValue * 1000 : numberValue;
  }

  return Date.parse(text);
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
      firstStoredAt: nowIso(),
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

  findRecentEvents({
    userId,
    message,
    pageId,
    postId,
    withinHours = 24,
    excludeEventId = null
  }) {
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

      if (pageId && event.pageId !== pageId) {
        return false;
      }

      if (postId && event.postId !== postId) {
        return false;
      }

      const createdTime = parseEventTime(event.createdAt || event.receivedAt || event.updatedAt || event.firstStoredAt);
      return Number.isFinite(createdTime) && createdTime >= windowStart;
    });
  }

  hasReplyForComment(commentId, excludeEventId = null) {
    if (!commentId) {
      return false;
    }

    return this.listEvents().some((event) => {
      if (!event || event.commentId !== commentId) {
        return false;
      }

      if (excludeEventId && event.eventId === excludeEventId) {
        return false;
      }

      const actionResults = Array.isArray(event.actionResults) ? event.actionResults : [];
      return actionResults.some((item) => {
        const action = item && item.action;
        const result = item && item.result;
        return action &&
          action.type === "reply_comment" &&
          result &&
          ["sent", "simulated", "duplicate_skipped"].includes(result.status);
      });
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

class ReplyGuardStore {
  constructor(baseDir = path.join(process.cwd(), "data")) {
    this.filePath = path.join(baseDir, "reply-guard.json");
    ensureDirectory(baseDir);
  }

  read() {
    return readJsonFile(this.filePath, {});
  }

  get(commentId) {
    if (!commentId) {
      return null;
    }

    return this.read()[commentId] || null;
  }

  reserve(commentId, payload = {}) {
    if (!commentId) {
      return null;
    }

    const records = this.read();

    if (records[commentId]) {
      return {
        reserved: false,
        record: records[commentId]
      };
    }

    records[commentId] = {
      commentId,
      status: "reserved",
      reservedAt: nowIso(),
      ...payload
    };

    writeJsonAtomic(this.filePath, records);

    return {
      reserved: true,
      record: records[commentId]
    };
  }

  mark(commentId, patch = {}) {
    if (!commentId) {
      return null;
    }

    const records = this.read();
    const current = records[commentId] || {
      commentId,
      reservedAt: nowIso()
    };

    records[commentId] = {
      ...current,
      ...patch,
      updatedAt: nowIso()
    };

    writeJsonAtomic(this.filePath, records);
    return records[commentId];
  }
}

module.exports = {
  EventStore,
  BlacklistStore,
  ReplyGuardStore,
  parseEventTime
};
