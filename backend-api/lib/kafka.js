const { Kafka } = require("kafkajs");
const config = require("./config");

function createKafka(clientId = config.kafkaClientId) {
  return new Kafka({
    clientId,
    brokers: [config.kafkaBroker],
    connectionTimeout: config.kafka.connectionTimeoutMs,
    requestTimeout: config.kafka.requestTimeoutMs,
    retry: {
      initialRetryTime: config.kafka.initialRetryTimeMs,
      retries: config.kafka.retries
    }
  });
}

function buildTopicSpecs(topicNames) {
  return topicNames.map((topic) => ({
    topic,
    numPartitions: config.kafka.topicPartitions[topic] || config.kafka.defaultTopicPartitions,
    replicationFactor: config.kafka.replicationFactor
  }));
}

async function ensureTopics(kafka, topicNames) {
  const admin = kafka.admin();

  await admin.connect();

  try {
    const topicSpecs = buildTopicSpecs(topicNames);
    const existingTopics = await admin.listTopics();
    const missingTopics = topicSpecs.filter((spec) => !existingTopics.includes(spec.topic));

    if (missingTopics.length > 0) {
      await admin.createTopics({
        waitForLeaders: true,
        topics: missingTopics
      });
    }

    const metadata = await admin.fetchTopicMetadata({ topics: topicNames });
    const partitionUpdates = [];

    for (const topic of metadata.topics) {
      const expectedPartitions = config.kafka.topicPartitions[topic.name] || config.kafka.defaultTopicPartitions;
      const currentPartitions = Array.isArray(topic.partitions) ? topic.partitions.length : 0;

      // Kafka chỉ cho tăng số partition, không cho giảm. Nếu topic đã tồn tại 1 partition
      // thì hàm này sẽ tự tăng lên theo cấu hình để consumer scale được khi bài post viral.
      if (expectedPartitions > currentPartitions) {
        partitionUpdates.push({
          topic: topic.name,
          count: expectedPartitions
        });
      }
    }

    if (partitionUpdates.length > 0) {
      await admin.createPartitions({
        validateOnly: false,
        topicPartitions: partitionUpdates
      });
    }
  } finally {
    await admin.disconnect();
  }
}

function createReliableProducer(kafka) {
  return kafka.producer({
    allowAutoTopicCreation: false,
    idempotent: true,
    maxInFlightRequests: 1,
    retry: {
      initialRetryTime: config.kafka.initialRetryTimeMs,
      retries: config.kafka.retries
    }
  });
}

module.exports = {
  createKafka,
  ensureTopics,
  createReliableProducer
};
