import { PubSub } from 'graphql-subscriptions';

const pubsub = new PubSub();
const QUEUE_UPDATED_TOPIC = 'QUEUE_UPDATED';

/**
 * An abstraction layer for the publish/subscribe system.
 * Currently uses an in-memory PubSub, but can be replaced
 * with a Kafka-backed implementation later.
 */
export const eventBus = {
  /**
   * Publishes a queue update event.
   * @param {object} eventPayload - The payload for the queueUpdated subscription.
   */
  publishQueueUpdate(eventPayload) {
    console.log(`📣 Publishing ${eventPayload.type} event for song ${eventPayload.songId}`);
    pubsub.publish(QUEUE_UPDATED_TOPIC, { queueUpdated: eventPayload });
  },

  /**
   * Returns an async iterator for the queue update topic.
   * @returns {AsyncIterator<any>}
   */
  asyncIterator() {
    return pubsub.asyncIterableIterator([QUEUE_UPDATED_TOPIC]);
  }
};