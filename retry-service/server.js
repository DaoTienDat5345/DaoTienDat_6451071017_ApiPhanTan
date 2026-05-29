require("dotenv").config();

const config = require("./lib/config");
const { createKafka, ensureTopics, createReliableProducer } = require("./lib/kafka");
const { sleep, nowIso } = require("./lib/utils");

const kafka = createKafka("retry-service");
const consumer = kafka.consumer({ groupId: config.retryGroupId });
const producer = createReliableProducer(kafka);

function getCommandId(payload) {
  return payload.command_id || payload.commandId || (payload.payload && (payload.payload.command_id || payload.payload.commandId));
}

function getEventId(payload) {
  return payload.event_id || payload.eventId || (payload.payload && (payload.payload.event_id || payload.payload.eventId));
}

function getRetryCount(payload) {
  return Number(payload.retry_count ?? payload.retryCount ?? 0);
}

function buildDelayMs(retryCount) {
  // exponential backoff: lần retry tiếp theo: 1s, 2s, 4s ... theo cấu hình baseDelayMs
  return config.retry.baseDelayMs * Math.pow(2, Math.max(0, retryCount));
}

async function publishStatus(eventId, commandId, status, extra = {}) {
  await producer.send({
    topic: config.topics.processingStatus,
    acks: -1,
    messages: [
      {
        key: eventId || commandId || `retry-${Date.now()}`,
        value: JSON.stringify({
          eventId: eventId || null,
          commandId: commandId || null,
          status,
          at: nowIso(),
          service: "retry-service",
          ...extra
        })
      }
    ]
  });
}

async function publishDeadLetter(payload, retryCount) {
  const commandId = getCommandId(payload);
  const eventId = getEventId(payload);
  const deadLetterPayload = {
    schema_version: 1,
    command_id: commandId,
    commandId,
    event_id: eventId,
    eventId,
    retry_count: retryCount,
    retryCount,
    failed_at: nowIso(),
    failedAt: nowIso(),
    final_error: payload.last_error || payload.lastError || payload.reason || "Maximum retry attempts exceeded",
    finalError: payload.last_error || payload.lastError || payload.reason || "Maximum retry attempts exceeded",
    original_topic: config.topics.sendFailed,
    originalTopic: config.topics.sendFailed,
    payload: payload.payload || payload
  };

  await producer.send({
    topic: config.topics.deadLetter,
    acks: -1,
    messages: [
      {
        key: commandId || eventId || `dead-${Date.now()}`,
        value: JSON.stringify(deadLetterPayload)
      }
    ]
  });

  await publishStatus(eventId, commandId, "dead_letter", {
    retryCount,
    finalError: deadLetterPayload.finalError
  });

  console.log(`[retry-service] Moved command=${commandId} to ${config.topics.deadLetter}`);
}

async function publishSendRetry(payload, nextRetryCount) {
  const commandId = getCommandId(payload);
  const eventId = getEventId(payload);
  const delayMs = buildDelayMs(nextRetryCount - 1);

  if (delayMs > 0) {
    console.log(`[retry-service] Waiting ${delayMs}ms before retrying command=${commandId}`);
    await sleep(delayMs);
  }

  const retryPayload = {
    ...payload,
    retry_count: nextRetryCount,
    retryCount: nextRetryCount,
    next_retry_at: nowIso(),
    nextRetryAt: nowIso(),
    payload: {
      ...(payload.payload || payload),
      retry_count: nextRetryCount,
      retryCount: nextRetryCount
    }
  };

  await producer.send({
    topic: config.topics.sendRetry,
    acks: -1,
    messages: [
      {
        key: commandId || eventId || `retry-${Date.now()}`,
        value: JSON.stringify(retryPayload)
      }
    ]
  });

  await publishStatus(eventId, commandId, "requeued", {
    retryCount: nextRetryCount,
    targetTopic: config.topics.sendRetry
  });

  console.log(`[retry-service] Requeued command=${commandId} to ${config.topics.sendRetry}`);
}

async function start() {
  await ensureTopics(kafka, Object.values(config.topics));
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: config.topics.sendFailed,
    fromBeginning: config.consumerFromBeginning
  });

  console.log(`Retry service is consuming topic ${config.topics.sendFailed}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      let payload;

      try {
        payload = JSON.parse(message.value.toString());
      } catch (error) {
        await producer.send({
          topic: config.topics.deadLetter,
          acks: -1,
          messages: [
            {
              key: message.key ? message.key.toString() : `bad-retry-${Date.now()}`,
              value: JSON.stringify({
                schema_version: 1,
                original_topic: config.topics.sendFailed,
                reason: `Cannot parse send_failed message: ${error.message}`,
                failed_at: nowIso(),
                raw_value: message.value ? message.value.toString() : null
              })
            }
          ]
        });
        return;
      }

      const retryCount = getRetryCount(payload);
      const nextRetryCount = retryCount + 1;

      if (payload.nonRetryable || nextRetryCount > config.retry.maxAttempts) {
        await publishDeadLetter(payload, retryCount);
        return;
      }

      await publishSendRetry(payload, nextRetryCount);
    }
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}, shutting down retry-service...`);

  try {
    await consumer.disconnect();
    await producer.disconnect();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start().catch((error) => {
  console.error("Failed to start retry-service:", error);
  process.exit(1);
});
