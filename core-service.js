require("dotenv").config();

const config = require("./lib/config");
const { ActionExecutor } = require("./lib/action-executor");
const { BlacklistStore, EventStore } = require("./lib/file-store");
const { createKafka, ensureTopics } = require("./lib/kafka");
const { processEvent } = require("./lib/pipeline");

const kafka = createKafka("core-service");
const consumer = kafka.consumer({ groupId: config.consumerGroupId });
const producer = kafka.producer();
const eventStore = new EventStore();
const blacklistStore = new BlacklistStore();
const actionExecutor = new ActionExecutor({ producer, blacklistStore });

async function start() {
  await ensureTopics(kafka, Object.values(config.topics));
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: config.topics.rawEvents,
    fromBeginning: true
  });

  console.log(`Core service is consuming topic ${config.topics.rawEvents}`);

  await consumer.run({
    eachMessage: async ({ message }) => {
      const payload = JSON.parse(message.value.toString());

      console.log(`[core-service] Processing event ${payload.eventId}`);

      try {
        await processEvent({
          event: payload,
          producer,
          eventStore,
          blacklistStore,
          actionExecutor
        });
      } catch (error) {
        console.error(`[core-service] Event ${payload.eventId} failed:`, error.message);
      }
    }
  });
}

async function shutdown(signal) {
  console.log(`Received ${signal}, shutting down core-service...`);

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
  console.error("Failed to start core-service:", error);
  process.exit(1);
});
