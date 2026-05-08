require("dotenv").config();

const config = require("./lib/config");
const { createKafka, ensureTopics } = require("./lib/kafka");
const { sleep } = require("./lib/utils");

const kafka = createKafka("retry-service");
const consumer = kafka.consumer({ groupId: config.retryGroupId });
const producer = kafka.producer();

async function publishStatus(eventId, status, extra = {}) {
  await producer.send({
    topic: config.topics.processingStatus,
    messages: [
      {
        key: eventId,
        value: JSON.stringify({
          eventId,
          status,
          at: new Date().toISOString(),
          ...extra
        })
      }
    ]
  });
}

async function start() {
  await ensureTopics(kafka, Object.values(config.topics));
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: config.topics.sendFailed,
    fromBeginning: true
  });

  console.log(`Retry service is consuming topic ${config.topics.sendFailed}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());
      const delayMs = Math.max(0, Number(payload.retryAt || 0) - Date.now());

      if (delayMs > 0) {
        console.log(
          `[retry-service] Waiting ${delayMs}ms before requeueing ${payload.event.eventId}`
        );
        await sleep(delayMs);
      }

      await producer.send({
        topic: config.topics.rawEvents,
        messages: [
          {
            key: payload.event.eventId,
            value: JSON.stringify({
              ...payload.event,
              retryCount: payload.retryCount
            })
          }
        ]
      });

      await publishStatus(payload.event.eventId, "requeued", {
        retryCount: payload.retryCount,
        stage: payload.stage || "unknown"
      });

      console.log(`[retry-service] Requeued ${payload.event.eventId}`);
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
