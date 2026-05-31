require("dotenv").config();

const config = require("./lib/config");
const { ActionExecutor } = require("./lib/action-executor");
const { BlacklistStore, EventStore, ReplyGuardStore } = require("./lib/file-store");
const { createKafka, ensureTopics, createReliableProducer } = require("./lib/kafka");
const { processEvent } = require("./lib/pipeline");
const { CommentStore } = require("./lib/comment-store");
const { closeDatabase } = require("./lib/db");

const kafka = createKafka("core-service");
const consumer = kafka.consumer({
  groupId: config.consumerGroupId,
  sessionTimeout: config.consumer.sessionTimeoutMs,
  heartbeatInterval: config.consumer.heartbeatIntervalMs,
  maxBytesPerPartition: config.consumer.maxBytesPerPartition,
  maxBytes: config.consumer.maxBytes,
  maxWaitTimeInMs: config.consumer.maxWaitTimeInMs,
  allowAutoTopicCreation: false
});
const producer = createReliableProducer(kafka);
const eventStore = new EventStore();
const blacklistStore = new BlacklistStore();
const replyGuardStore = new ReplyGuardStore();
const actionExecutor = new ActionExecutor({ producer, blacklistStore, replyGuardStore });
const commentStore = new CommentStore();

function safeParseMessage(message) {
  try {
    return {
      ok: true,
      payload: JSON.parse(message.value.toString())
    };
  } catch (error) {
    return {
      ok: false,
      error
    };
  }
}

async function publishBadMessageToDeadLetter(message, error, batch) {
  const payload = {
    event: null,
    retryCount: 0,
    reason: `Cannot parse raw_events message: ${error.message}`,
    stage: "json_parse",
    failedAt: new Date().toISOString(),
    kafka: {
      topic: batch.topic,
      partition: batch.partition,
      offset: message.offset,
      key: message.key ? message.key.toString() : null
    },
    rawValue: message.value ? message.value.toString() : null
  };

  await producer.send({
    topic: config.topics.deadLetter,
    acks: -1,
    messages: [
      {
        key: message.key ? message.key.toString() : `bad-message-${batch.partition}-${message.offset}`,
        value: JSON.stringify(payload)
      }
    ]
  });
}

async function handleMessage(message, batch) {
  const parsed = safeParseMessage(message);

  if (!parsed.ok) {
    await publishBadMessageToDeadLetter(message, parsed.error, batch);
    console.error(
      `[core-service] Bad JSON moved to dead_letter at ${batch.topic}[${batch.partition}]@${message.offset}:`,
      parsed.error.message
    );
    return;
  }

  const payload = parsed.payload;
  console.log(
    `[core-service] Processing ${payload.eventId} from ${batch.topic}[${batch.partition}]@${message.offset}`
  );

  try {
    await commentStore.upsertReceived(payload);
  } catch (dbError) {
    console.error(`[core-service] Cannot persist received comment ${payload.eventId}:`, dbError.message);
  }

  try {
    const result = await processEvent({
      event: payload,
      producer,
      eventStore,
      blacklistStore,
      actionExecutor
    });

    if (result && result.processedPayload) {
      try {
        await commentStore.upsertProcessed(result.processedPayload);
      } catch (dbError) {
        console.error(`[core-service] Cannot persist processed comment ${payload.eventId}:`, dbError.message);
      }

      console.log(
        "[core-service] Result",
        JSON.stringify({
          eventId: result.processedPayload.eventId,
          intent: result.processedPayload.intent,
          sentiment: result.processedPayload.sentiment,
          action: result.processedPayload.action,
          status: result.processedPayload.status
        })
      );
    } else if (result && result.skipped) {
      try {
        await commentStore.markIgnored(payload, result.reason);
      } catch (dbError) {
        console.error(`[core-service] Cannot persist ignored comment ${payload.eventId}:`, dbError.message);
      }

      console.log(`[core-service] Skipped ${payload.eventId}: ${result.reason}`);
    }
  } catch (error) {
    // processEvent đã tự ghi retry/dead_letter khi lỗi xử lý. Ở đây vẫn cho phép
    // commit offset để consumer không kẹt mãi tại 1 message độc hại.
    try {
      await commentStore.markFailed(payload, error);
    } catch (dbError) {
      console.error(`[core-service] Cannot persist failed comment ${payload.eventId}:`, dbError.message);
    }

    console.error(`[core-service] Event ${payload.eventId} failed after retry/DLQ handling:`, error.message);
  }
}

async function start() {
  await ensureTopics(kafka, Object.values(config.topics));
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: config.topics.rawEvents,
    fromBeginning: config.consumerFromBeginning
  });

  console.log(
    `Core service is consuming ${config.topics.rawEvents} with ${config.consumer.partitionsConsumedConcurrently} partition worker(s)`
  );

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,
    partitionsConsumedConcurrently: config.consumer.partitionsConsumedConcurrently,
    eachBatch: async ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }) => {
      let processedInBatch = 0;
      const messages = batch.messages.slice(0, config.consumer.maxBatchMessages);

      for (const message of messages) {
        if (!isRunning() || isStale()) {
          break;
        }

        await handleMessage(message, batch);

        // Chỉ resolve offset sau khi event đã xử lý xong hoặc đã được đưa vào retry/DLQ.
        // Nếu service chết trước dòng này, Kafka sẽ giao lại message nên không mất dữ liệu.
        resolveOffset(message.offset);
        processedInBatch += 1;

        if (processedInBatch % config.consumer.commitEveryMessages === 0) {
          await commitOffsetsIfNecessary();
        }

        await heartbeat();
      }

      await commitOffsetsIfNecessary();
      await heartbeat();
    }
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}, shutting down core-service...`);

  try {
    await consumer.disconnect();
    await producer.disconnect();
    await closeDatabase();
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

start().catch((error) => {
  console.error("Failed to start core-service:", error);
  process.exit(1);
});
