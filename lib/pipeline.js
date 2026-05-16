const config = require("./config");
const { classifyMessage } = require("./ai-classifier");
const { buildDecision } = require("./decision-engine");
const { detectSpam } = require("./spam-detector");
const { isRetryableError, nowIso, normalizeText } = require("./utils");

function buildRetryDelay(retryCount) {
  return config.retry.baseDelayMs * Math.max(1, retryCount);
}

async function publishStatus(producer, event, status, extra = {}) {
  const payload = {
    eventId: event.eventId,
    commentId: event.commentId || null,
    userId: event.fromUserId || null,
    status,
    at: nowIso(),
    ...extra
  };

  await producer.send({
    topic: config.topics.processingStatus,
    messages: [
      {
        key: event.eventId,
        value: JSON.stringify(payload)
      }
    ]
  });
}

function isLikelyOwnAutoReply(event) {
  const text = normalizeText(event.message || event.normalizedMessage || "");

  if (!text) {
    return false;
  }

  const botReplyPrefixes = [
    "shop da nhan duoc cau hoi ve gia",
    "shop co ho tro giao hang toan quoc",
    "cam on ban quan tam",
    "cam on ban rat nhieu",
    "shop rat tiec vi trai nghiem cua ban chua tot"
  ];

  return botReplyPrefixes.some((prefix) => text.startsWith(prefix));
}

function shouldIgnoreEvent(event) {
  if (!event) {
    return { ignored: true, reason: "empty_event" };
  }

  if (event.eventType !== "comment_created") {
    return { ignored: true, reason: `unsupported_event_type:${event.eventType || "unknown"}` };
  }

  // Facebook also sends webhook events for replies created by the Page bot.
  // If we process those replies again, the bot can reply to its own reply forever.
  if (event.isReplyToComment) {
    return { ignored: true, reason: "comment_reply_ignored" };
  }

  if (event.pageId && event.fromUserId && String(event.pageId) === String(event.fromUserId)) {
    return { ignored: true, reason: "page_own_comment_ignored" };
  }

  if (isLikelyOwnAutoReply(event)) {
    return { ignored: true, reason: "own_auto_reply_ignored" };
  }

  if (!event.message || !String(event.message).trim()) {
    return { ignored: true, reason: "empty_message" };
  }

  return { ignored: false, reason: null };
}

function mapAction(decision) {
  if (!decision || !decision.route) {
    return "unknown";
  }

  if (decision.route === "auto_reply") {
    return "auto_reply";
  }

  if (decision.route === "manual_review") {
    return "send_to_support";
  }

  if (decision.route === "spam_handling") {
    const actions = Array.isArray(decision.actions) ? decision.actions : [];
    if (actions.some((action) => action.type === "blacklist_user")) {
      return "blacklist_user";
    }
    if (actions.some((action) => action.type === "manual_review")) {
      return "hide_and_review";
    }
    return "hide_comment";
  }

  if (decision.route === "observe_only") {
    return "observe_only";
  }

  return decision.route;
}

async function publishProcessedEvent(producer, event, payload) {
  await producer.send({
    topic: config.topics.processedEvents,
    messages: [
      {
        key: event.eventId,
        value: JSON.stringify(payload)
      }
    ]
  });
}

async function publishRetry(producer, event, error, retryCount) {
  const retryAt = Date.now() + buildRetryDelay(retryCount);
  const payload = {
    event,
    retryCount,
    retryAt,
    reason: error.message,
    stage: error.stage || "unknown",
    failedAt: nowIso()
  };

  await producer.send({
    topic: config.topics.sendFailed,
    messages: [
      {
        key: event.eventId,
        value: JSON.stringify(payload)
      }
    ]
  });
}

async function publishDeadLetter(producer, event, error, retryCount) {
  const payload = {
    event,
    retryCount,
    reason: error.message,
    stage: error.stage || "unknown",
    failedAt: nowIso()
  };

  await producer.send({
    topic: config.topics.deadLetter,
    messages: [
      {
        key: event.eventId,
        value: JSON.stringify(payload)
      }
    ]
  });
}

async function processEvent({
  event,
  producer,
  eventStore,
  blacklistStore,
  actionExecutor
}) {
  const retryCount = Number(event.retryCount || 0);
  const existingEvent = eventStore.getEvent(event.eventId);
  const terminalStatuses = [
    "processed",
    "replied",
    "action_sent",
    "action_failed",
    "ignored",
    "duplicate_ignored"
  ];

  if (existingEvent && terminalStatuses.includes(existingEvent.status)) {
    await publishStatus(producer, event, "duplicate_ignored", {
      retryCount,
      previousStatus: existingEvent.status
    });
    return {
      skipped: true,
      reason: "duplicate_event"
    };
  }

  const ignoreCheck = shouldIgnoreEvent(event);
  if (ignoreCheck.ignored) {
    eventStore.upsertEvent(event.eventId, {
      ...event,
      status: "ignored",
      ignoredReason: ignoreCheck.reason,
      retryCount,
      note: "Event ignored before automation to prevent reply loops"
    });
    await publishStatus(producer, event, "ignored", {
      retryCount,
      reason: ignoreCheck.reason
    });
    return {
      skipped: true,
      reason: ignoreCheck.reason
    };
  }

  eventStore.upsertEvent(event.eventId, {
    ...event,
    status: "received",
    note: "Event persisted by core-service"
  });
  await publishStatus(producer, event, "received", { retryCount });

  try {
    eventStore.upsertEvent(event.eventId, {
      status: "processing",
      note: "Pipeline started"
    });
    await publishStatus(producer, event, "processing", { retryCount });

    const recentSameUserEvents = eventStore.findRecentEvents({
      userId: event.fromUserId,
      withinHours: 24,
      excludeEventId: event.eventId
    });
    const recentSameMessageEvents = eventStore.findRecentEvents({
      userId: event.fromUserId,
      message: event.normalizedMessage,
      withinHours: 24,
      excludeEventId: event.eventId
    });
    const recentSamePageMessageEvents = eventStore.findRecentEvents({
      pageId: event.pageId,
      message: event.normalizedMessage,
      withinHours: 24,
      excludeEventId: event.eventId
    });

    const spamAnalysis = detectSpam(event, {
      recentSameUserEvents,
      recentSameMessageEvents,
      recentSamePageMessageEvents,
      isBlacklisted: blacklistStore.isBlacklisted(event.fromUserId)
    });

    if (eventStore.hasReplyForComment(event.commentId, event.eventId)) {
      spamAnalysis.reasons.push("comment_already_auto_replied");
      spamAnalysis.signals.commentAlreadyAutoReplied = true;
    }
    const classification = await classifyMessage(event.message, spamAnalysis);
    const decision = buildDecision(event, spamAnalysis, classification);

    if (spamAnalysis.signals.commentAlreadyAutoReplied && Array.isArray(decision.actions)) {
      decision.actions = decision.actions.filter((action) => action.type !== "reply_comment");
      decision.autoReplySuppressed = true;
      decision.summary = `${decision.summary}; auto reply suppressed because comment was already replied`;
      if (decision.route === "auto_reply") {
        decision.route = "observe_only";
      }
    }

    const actionResults = await actionExecutor.execute(event, decision);

    const hasFailedAction = actionResults.some((item) => item.result && item.result.status === "failed");
    const hasReplyAction = actionResults.some((item) => item.action && item.action.type === "reply_comment");

    const finalStatus = hasFailedAction
      ? "action_failed"
      : hasReplyAction
        ? "replied"
        : actionResults.length > 0
          ? "action_sent"
          : "processed";

    const processedPayload = {
      eventId: event.eventId,
      source: event.source || null,
      eventType: event.eventType || null,
      pageId: event.pageId || null,
      postId: event.postId || null,
      commentId: event.commentId || null,
      userId: event.fromUserId || null,
      message: event.message || "",
      normalizedMessage: event.normalizedMessage || "",
      isSpam: spamAnalysis.isSpam,
      spamReason: spamAnalysis.reasons.length ? spamAnalysis.reasons.join(",") : null,
      spamAnalysis,
      intent: classification.intent,
      sentiment: classification.sentiment,
      confidence: classification.confidence,
      classifierSource: classification.source,
      action: mapAction(decision),
      decision,
      actionResults,
      status: finalStatus,
      processedAt: nowIso(),
      retryCount
    };

    const persisted = eventStore.upsertEvent(event.eventId, {
      status: finalStatus,
      spamAnalysis,
      classification,
      decision,
      actionResults,
      processedPayload,
      retryCount,
      note: "Pipeline finished"
    });

    await publishStatus(producer, event, finalStatus, {
      retryCount,
      decision: decision.summary
    });

    await publishProcessedEvent(producer, event, processedPayload);

    return persisted;
  } catch (error) {
    const retryable = isRetryableError(error);
    const nextRetryCount = retryCount + 1;

    if (retryable && nextRetryCount <= config.retry.maxAttempts) {
      eventStore.upsertEvent(event.eventId, {
        status: "retrying",
        retryCount: nextRetryCount,
        lastError: error.message,
        lastErrorStage: error.stage || "unknown",
        note: "Queued for retry"
      });

      await publishRetry(
        producer,
        {
          ...event,
          retryCount: nextRetryCount
        },
        error,
        nextRetryCount
      );
      await publishStatus(producer, event, "retrying", {
        retryCount: nextRetryCount,
        reason: error.message
      });

      return {
        queuedForRetry: true,
        retryCount: nextRetryCount
      };
    }

    eventStore.upsertEvent(event.eventId, {
      status: "failed",
      retryCount,
      lastError: error.message,
      lastErrorStage: error.stage || "unknown",
      note: "Moved to dead letter or failed permanently"
    });

    await publishDeadLetter(producer, event, error, retryCount);
    await publishStatus(producer, event, "failed", {
      retryCount,
      reason: error.message
    });

    throw error;
  }
}

module.exports = {
  processEvent
};
