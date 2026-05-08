const { Kafka } = require("kafkajs");
const config = require("./config");

function createKafka(clientId = config.kafkaClientId) {
  return new Kafka({
    clientId,
    brokers: [config.kafkaBroker]
  });
}

async function ensureTopics(kafka, topicNames) {
  const admin = kafka.admin();

  await admin.connect();

  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: topicNames.map((topic) => ({
        topic,
        numPartitions: 1,
        replicationFactor: 1
      }))
    });
  } finally {
    await admin.disconnect();
  }
}

module.exports = {
  createKafka,
  ensureTopics
};
