import { PubSub } from 'graphql-subscriptions';
import { Kafka } from 'kafkajs';

// In-memory fallback
const pubsub = new PubSub();
const IN_MEMORY_TOPIC = 'QUEUE_UPDATED';

// Kafka configuration from environment variables
const {
  KAFKA_ENABLE,
  KAFKA_BROKERS,
  KAFKA_CLIENT_ID,
  KAFKA_QUEUE_TOPIC
} = process.env;

const useKafka = KAFKA_ENABLE === 'true' && KAFKA_BROKERS;
let kafkaProducer = null;
let kafkaConsumer = null; // Keep a reference to the consumer
let kafkaConsumerStarted = false;

/**
 * Initializes the Kafka producer and consumer.
 * Connects to the Kafka brokers and subscribes to the queue topic.
 */
async function initializeKafka() {
  if (!useKafka) {
    console.log('🧩 Event bus mode: in-memory');
    return;
  }

  try {
    const kafka = new Kafka({
      clientId: KAFKA_CLIENT_ID || 'nerdbox-gateway',
      brokers: KAFKA_BROKERS.split(','),
    });

    // Initialize Producer
    const producer = kafka.producer();
    await producer.connect();
    kafkaProducer = producer;
    console.log('🟢 Kafka Producer connected.');

    // Initialize Consumer
    const consumer = kafka.consumer({ groupId: `${KAFKA_CLIENT_ID}-group` });
    await consumer.connect();
    kafkaConsumer = consumer; // Store the consumer reference
    await consumer.subscribe({ topic: KAFKA_QUEUE_TOPIC, fromBeginning: false });
    console.log(`👂 Kafka Consumer subscribed to topic: ${KAFKA_QUEUE_TOPIC}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const eventPayload = JSON.parse(message.value.toString());
          console.log(`📥 Received event from Kafka topic "${topic}":`, eventPayload.type);
          // When a message is received from Kafka, publish it to the local
          // in-memory PubSub for all connected WebSocket clients of this instance.
          pubsub.publish(IN_MEMORY_TOPIC, { queueUpdated: eventPayload });
        } catch (err) {
          console.error('Error processing Kafka message:', err);
        }
      },
    });
    kafkaConsumerStarted = true;
    console.log('🟢 Kafka Consumer is running.');

  } catch (error) {
    console.error('🔴 Kafka initialization failed. Falling back to in-memory mode.', error);
    // Ensure we don't try to use a partially initialized producer
    kafkaProducer = null; 
  }
}

export const eventBus = {
  /**
   * Starts the event bus, initializing Kafka if enabled.
   */
  start: initializeKafka,

  /**
   * Gracefully disconnects Kafka clients.
   */
  async shutdown() {
    if (kafkaProducer) {
      await kafkaProducer.disconnect();
      console.log('⚪ Kafka Producer disconnected.');
    }
    if (kafkaConsumer) {
      await kafkaConsumer.disconnect();
      console.log('⚪ Kafka Consumer disconnected.');
    }
  },

  /**
   * Publishes a queue update event.
   * It always publishes to the local in-memory PubSub for immediate delivery.
   * If Kafka is enabled, it also sends the event to the Kafka topic.
   * @param {object} eventPayload - The payload for the queueUpdated subscription.
   */
  async publishQueueUpdate(eventPayload) {
    // 1. Always publish locally for immediate feedback to this instance's clients.
    pubsub.publish(IN_MEMORY_TOPIC, { queueUpdated: eventPayload });

    // 2. If Kafka is enabled, send the event to the topic for other instances.
    if (kafkaProducer) {
      try {
        await kafkaProducer.send({
          topic: KAFKA_QUEUE_TOPIC,
          messages: [{ value: JSON.stringify(eventPayload) }],
        });
        console.log(`📤 Sent event to Kafka topic "${KAFKA_QUEUE_TOPIC}":`, eventPayload.type);
      } catch (err) {
        console.error('Error publishing to Kafka:', err);
      }
    }
  },

  /**
   * Returns an async iterator for the local in-memory queue update topic.
   * @returns {AsyncIterator<any>}
   */
  asyncIterator() {
    return pubsub.asyncIterableIterator([IN_MEMORY_TOPIC]);
  }
};