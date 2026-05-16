const { nowIso, normalizeText, stableHash } = require("./utils");

function extractMessage(value) {
  if (!value) {
    return "";
  }

  if (typeof value.message === "string") {
    return value.message;
  }

  if (typeof value.comment_text === "string") {
    return value.comment_text;
  }

  if (typeof value.description === "string") {
    return value.description;
  }

  if (value.message && typeof value.message.text === "string") {
    return value.message.text;
  }

  return "";
}

function inferEventType(field, value) {
  if (value && value.item === "comment" && value.verb === "add") {
    return "comment_created";
  }

  if (value && value.item === "comment" && value.verb === "edited") {
    return "comment_updated";
  }

  if (value && value.item === "post") {
    return "post_activity";
  }

  if (field === "feed") {
    return "feed_change";
  }

  if (field === "messages") {
    return "message_event";
  }

  return "unknown_event";
}

function buildEventId(source, eventType, preferredKey, rawSeed) {
  if (preferredKey) {
    return `${source}:${eventType}:${preferredKey}`;
  }

  return `${source}:${eventType}:${stableHash(rawSeed).slice(0, 20)}`;
}

function buildNormalizedEvent(entry, change, indexes) {
  const value = change.value || {};
  const message = extractMessage(value);
  const eventType = inferEventType(change.field, value);
  const commentId = value.comment_id || value.target_id || null;
  const postId = value.post_id || (value.post && value.post.id) || null;
  const parentId = value.parent_id || null;
  const isReplyToComment = Boolean(commentId && postId && parentId && parentId !== postId);
  const fromUserId =
    (value.from && value.from.id) ||
    value.sender_id ||
    (value.sender && value.sender.id) ||
    null;
  const fromName = (value.from && value.from.name) || null;
  const pageId = entry.id || value.page_id || null;
  const preferredKey = commentId || postId || `${pageId || "page"}-${entry.time || Date.now()}-${indexes.changeIndex}`;

  return {
    eventId: buildEventId("facebook", eventType, preferredKey, { entry, change, indexes }),
    source: "facebook",
    eventType,
    field: change.field || null,
    pageId,
    postId,
    commentId,
    parentId,
    isReplyToComment,
    fromUserId,
    fromName,
    verb: value.verb || null,
    item: value.item || null,
    message,
    normalizedMessage: normalizeText(message),
    createdAt: value.created_time || (entry.time ? new Date(entry.time * 1000).toISOString() : nowIso()),
    receivedAt: nowIso(),
    retryCount: 0,
    raw: {
      entry,
      change
    }
  };
}

function buildMessagingEvent(entry, messageEvent, indexes) {
  const message = extractMessage(messageEvent);
  const senderId = messageEvent.sender && messageEvent.sender.id;
  const recipientId = messageEvent.recipient && messageEvent.recipient.id;
  const preferredKey =
    (messageEvent.message && messageEvent.message.mid) ||
    `${senderId || "sender"}-${messageEvent.timestamp || Date.now()}-${indexes.messageIndex}`;

  return {
    eventId: buildEventId("facebook", "messaging_event", preferredKey, { entry, messageEvent, indexes }),
    source: "facebook",
    eventType: "messaging_event",
    field: "messages",
    pageId: recipientId || entry.id || null,
    postId: null,
    commentId: null,
    fromUserId: senderId || null,
    verb: null,
    item: "message",
    message,
    normalizedMessage: normalizeText(message),
    createdAt: messageEvent.timestamp ? new Date(messageEvent.timestamp).toISOString() : nowIso(),
    receivedAt: nowIso(),
    retryCount: 0,
    raw: {
      entry,
      messageEvent
    }
  };
}

function normalizeFacebookPayload(payload) {
  const entries = Array.isArray(payload.entry) ? payload.entry : [];
  const normalizedEvents = [];

  entries.forEach((entry, entryIndex) => {
    const changes = Array.isArray(entry.changes) ? entry.changes : [];
    const messaging = Array.isArray(entry.messaging) ? entry.messaging : [];

    changes.forEach((change, changeIndex) => {
      normalizedEvents.push(buildNormalizedEvent(entry, change, { entryIndex, changeIndex }));
    });

    messaging.forEach((messageEvent, messageIndex) => {
      normalizedEvents.push(buildMessagingEvent(entry, messageEvent, { entryIndex, messageIndex }));
    });
  });

  if (normalizedEvents.length > 0) {
    return normalizedEvents;
  }

  return [
    {
      eventId: buildEventId("facebook", "raw_payload", null, payload),
      source: "facebook",
      eventType: "raw_payload",
      field: payload.object || null,
      pageId: null,
      postId: null,
      commentId: null,
      fromUserId: null,
      verb: null,
      item: null,
      message: "",
      normalizedMessage: "",
      createdAt: nowIso(),
      receivedAt: nowIso(),
      retryCount: 0,
      raw: payload
    }
  ];
}

module.exports = {
  normalizeFacebookPayload
};
