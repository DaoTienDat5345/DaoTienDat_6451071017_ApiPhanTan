require("dotenv").config();

const express = require("express");
const config = require("./lib/config");
const { createKafka, ensureTopics, createReliableProducer } = require("./lib/kafka");
const { FileIdempotencyStore } = require("./lib/idempotency-store");
const { createFacebookClient } = require("./lib/facebook-client");
const { CircuitBreaker } = require("./lib/circuit-breaker");
const { isRetryableError, nowIso } = require("./lib/utils");

const app = express();
app.use(express.json({ limit: "2mb" }));

const kafka = createKafka("backend-api");
const producer = createReliableProducer(kafka);
const consumer = kafka.consumer({ groupId: config.backendGroupId });
const idempotencyStore = new FileIdempotencyStore();
const facebookClient = createFacebookClient();
const facebookBreaker = new CircuitBreaker({
  failureThreshold: config.circuitBreaker.failureThreshold,
  openTimeoutMs: config.circuitBreaker.openTimeoutMs
});

function normalizeCommand(input) {
  const command = input || {};
  const target = command.target || {};
  const payload = command.payload || {};
  const action = command.action || payload.action;
  const eventId = command.eventId || command.event_id || payload.eventId || payload.event_id;
  const commentId =
    target.commentId ||
    target.comment_id ||
    command.commentId ||
    command.comment_id ||
    payload.commentId ||
    payload.comment_id;
  const pageId =
    target.pageId ||
    target.page_id ||
    command.pageId ||
    command.page_id ||
    payload.pageId ||
    payload.page_id ||
    config.facebook.pageId;
  const replyText =
    command.replyText ||
    command.reply_text ||
    payload.replyText ||
    payload.reply_text ||
    payload.message ||
    command.message;
  const commandId =
    command.commandId ||
    command.command_id ||
    payload.commandId ||
    payload.command_id ||
    `${eventId || "unknown"}:${action || "unknown"}:${commentId || pageId || "target"}`;

  return {
    ...command,
    commandId,
    command_id: commandId,
    eventId,
    event_id: eventId,
    action,
    target: {
      ...target,
      pageId,
      page_id: pageId,
      commentId,
      comment_id: commentId
    },
    replyText,
    reply_text: replyText,
    retryCount: Number(command.retryCount ?? command.retry_count ?? 0),
    retry_count: Number(command.retryCount ?? command.retry_count ?? 0),
    payload: payload && Object.keys(payload).length ? payload : command.payload
  };
}

function extractErrorDetails(error) {
  if (!error) {
    return { message: "Unknown error" };
  }

  if (error.response) {
    return {
      message: error.message,
      httpStatus: error.response.status,
      facebookResponse: error.response.data || null
    };
  }

  return {
    message: error.message,
    code: error.code || null
  };
}

function isNonRetryableFacebookError(error) {
  const status = error && error.response && error.response.status;
  if (status && status >= 400 && status < 500 && status !== 408 && status !== 409 && status !== 425 && status !== 429) {
    return true;
  }

  return ["INVALID_COMMAND", "MISSING_PAGE_ACCESS_TOKEN"].includes(error && error.code);
}

async function publishStatus(command, status, extra = {}) {
  await producer.send({
    topic: config.topics.processingStatus,
    acks: -1,
    messages: [
      {
        key: command.eventId || command.commandId,
        value: JSON.stringify({
          eventId: command.eventId || null,
          commandId: command.commandId,
          status,
          at: nowIso(),
          service: "backend-api",
          ...extra
        })
      }
    ]
  });
}

async function publishSendFailed(command, error, { nonRetryable = false } = {}) {
  const retryCount = Number(command.retryCount || command.retry_count || 0);
  const payload = {
    schema_version: 1,
    command_id: command.commandId,
    commandId: command.commandId,
    event_id: command.eventId || null,
    eventId: command.eventId || null,
    retry_count: retryCount,
    retryCount,
    last_error: error.message,
    lastError: error.message,
    nonRetryable,
    failed_at: nowIso(),
    failedAt: nowIso(),
    payload: command
  };

  await producer.send({
    topic: nonRetryable ? config.topics.deadLetter : config.topics.sendFailed,
    acks: -1,
    messages: [
      {
        key: command.commandId,
        value: JSON.stringify(payload)
      }
    ]
  });

  await publishStatus(command, nonRetryable ? "dead_letter" : "send_failed", {
    retryCount,
    error: extractErrorDetails(error)
  });
}

async function executeFacebookCommand(command) {
  switch (command.action) {
    case "reply":
    case "reply_comment":
    case "auto_reply":
      return facebookClient.replyComment(command.target.commentId, command.replyText);

    case "hide":
    case "hide_comment":
      return facebookClient.hideComment(command.target.commentId);

    case "post":
    case "create_post":
      return facebookClient.createPost(command.target.pageId, command.replyText || command.message);

    case "manual_review":
      return {
        skipped: true,
        reason: "manual_review is queued for admin, no Facebook API call"
      };

    default: {
      const error = new Error(`Unsupported backend action: ${command.action}`);
      error.code = "INVALID_COMMAND";
      throw error;
    }
  }
}

async function handleCommandMessage(rawPayload) {
  const command = normalizeCommand(rawPayload);

  if (!command.commandId) {
    const error = new Error("Missing command_id");
    error.code = "INVALID_COMMAND";
    throw error;
  }

  console.log(`[backend-api] command=${command.commandId} action=${command.action} retry=${command.retryCount}`);

  if (await idempotencyStore.isProcessed(command.commandId)) {
    const previous = await idempotencyStore.get(command.commandId);
    console.log(`[backend-api] duplicate skipped command=${command.commandId}`);
    await publishStatus(command, "duplicate_skipped", { previous });
    return { skipped: true, reason: "idempotency_key_exists" };
  }

  if (!facebookBreaker.canRequest()) {
    const breakerState = facebookBreaker.getState();
    const error = new Error(`Facebook circuit breaker is ${breakerState.state}`);
    error.code = "CIRCUIT_BREAKER_OPEN";
    await publishSendFailed(command, error);
    return { queuedForRetry: true, reason: "circuit_breaker_open" };
  }

  try {
    const facebookResponse = await executeFacebookCommand(command);
    facebookBreaker.recordSuccess();

    await idempotencyStore.markProcessed(command.commandId, {
      eventId: command.eventId || null,
      action: command.action,
      target: command.target,
      facebookResponse
    });

    await publishStatus(command, "sent", {
      action: command.action,
      facebookResponse
    });

    console.log(`[backend-api] sent command=${command.commandId}`);
    return { success: true, facebookResponse };
  } catch (error) {
    facebookBreaker.recordFailure();
    const nonRetryable = isNonRetryableFacebookError(error) || !isRetryableError(error);

    console.error(`[backend-api] failed command=${command.commandId}:`, extractErrorDetails(error));
    await publishSendFailed(command, error, { nonRetryable });
    return { queuedForRetry: !nonRetryable, nonRetryable, error: error.message };
  }
}

async function safeHandleKafkaMessage(message, topic) {
  try {
    const payload = JSON.parse(message.value.toString());

    // send_retry wraps the original command inside payload, while reply_commands is already the command.
    const commandPayload = payload.payload && (payload.command_id || payload.commandId)
      ? {
          ...payload.payload,
          retryCount: Number(payload.retry_count ?? payload.retryCount ?? 0),
          retry_count: Number(payload.retry_count ?? payload.retryCount ?? 0)
        }
      : payload;

    await handleCommandMessage(commandPayload);
  } catch (error) {
    console.error(`[backend-api] Cannot process message from ${topic}:`, error.message);
    await producer.send({
      topic: config.topics.deadLetter,
      acks: -1,
      messages: [
        {
          key: message.key ? message.key.toString() : `bad-${Date.now()}`,
          value: JSON.stringify({
            schema_version: 1,
            original_topic: topic,
            reason: error.message,
            failed_at: nowIso(),
            raw_value: message.value ? message.value.toString() : null
          })
        }
      ]
    });
  }
}

app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] backend-api ${req.method} ${req.originalUrl}`);
  next();
});

app.get("/health", (req, res) => {
  res.json({
    success: true,
    service: "backend-api",
    port: config.backendPort,
    facebookSimulated: config.facebook.simulateActions,
    circuitBreaker: facebookBreaker.getState(),
    topics: {
      consume: [config.topics.replyCommands, config.topics.sendRetry],
      produceOnFailure: config.topics.sendFailed
    }
  });
});

app.get("/posts", async (req, res) => {
  try {
    const pageId = req.query.pageId || req.query.page_id || config.facebook.pageId;
    const limit = Number(req.query.limit || 20);
    const data = await facebookClient.getPosts(pageId, limit);
    res.json({ success: true, data });
  } catch (error) {
    res.status(error.response ? error.response.status : 500).json({
      success: false,
      code: error.code || "FACEBOOK_API_ERROR",
      message: error.message,
      detail: extractErrorDetails(error)
    });
  }
});

app.post("/post", async (req, res) => {
  try {
    const pageId = req.body.pageId || req.body.page_id || config.facebook.pageId;
    const message = req.body.message;
    const data = await facebookClient.createPost(pageId, message);
    res.json({ success: true, data });
  } catch (error) {
    res.status(error.response ? error.response.status : 500).json({
      success: false,
      code: error.code || "FACEBOOK_API_ERROR",
      message: error.message,
      detail: extractErrorDetails(error)
    });
  }
});

app.get("/comments", async (req, res) => {
  try {
    const postId = req.query.postId || req.query.post_id;
    const limit = Number(req.query.limit || 50);
    const data = await facebookClient.getComments(postId, limit);
    res.json({ success: true, data });
  } catch (error) {
    res.status(error.response ? error.response.status : 500).json({
      success: false,
      code: error.code || "FACEBOOK_API_ERROR",
      message: error.message,
      detail: extractErrorDetails(error)
    });
  }
});

app.post("/commands/test", async (req, res) => {
  const result = await handleCommandMessage(req.body);
  res.json({ success: true, result });
});

async function startKafkaConsumer() {
  await ensureTopics(kafka, Object.values(config.topics));
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: config.topics.replyCommands, fromBeginning: config.consumerFromBeginning });
  await consumer.subscribe({ topic: config.topics.sendRetry, fromBeginning: config.consumerFromBeginning });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      await safeHandleKafkaMessage(message, topic);
    }
  });

  console.log(`[backend-api] consuming ${config.topics.replyCommands}, ${config.topics.sendRetry}`);
}

async function start() {
  await startKafkaConsumer();

  app.listen(config.backendPort, () => {
    console.log(`Backend API running at http://localhost:${config.backendPort}`);
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}, shutting down backend-api...`);

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
  console.error("Failed to start backend-api:", error);
  process.exit(1);
});
