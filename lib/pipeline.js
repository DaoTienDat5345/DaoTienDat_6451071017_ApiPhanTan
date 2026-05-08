const config = require("./config");
const { classifyMessage } = require("./ai-classifier");
const { buildDecision } = require("./decision-engine");
const { detectSpam } = require("./spam-detector");
const { isRetryableError, nowIso } = require("./utils");

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

  if (existingEvent && ["processed", "replied", "action_sent"].includes(existingEvent.status)) {
    await publishStatus(producer, event, "duplicate_ignored", {
      retryCount
    });
    return {
      skipped: true,
      reason: "duplicate_event"
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

    const spamAnalysis = detectSpam(event, {
      recentSameUserEvents,
      recentSameMessageEvents,
      isBlacklisted: blacklistStore.isBlacklisted(event.fromUserId)
    });
    const classification = await classifyMessage(event.message, spamAnalysis);
    const decision = buildDecision(event, spamAnalysis, classification);
    const actionResults = await actionExecutor.execute(event, decision);

    const finalStatus = actionResults.some((item) => item.action.type === "reply_comment")
      ? "replied"
      : actionResults.length > 0
        ? "action_sent"
        : "processed";

    const persisted = eventStore.upsertEvent(event.eventId, {
      status: finalStatus,
      spamAnalysis,
      classification,
      decision,
      actionResults,
      retryCount,
      note: "Pipeline finished"
    });

    await publishStatus(producer, event, finalStatus, {
      retryCount,
      decision: decision.summary
    });

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
