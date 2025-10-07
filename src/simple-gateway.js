import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import { ApolloServerPluginDrainHttpServer } from '@apollo/server/plugin/drainHttpServer';
import { makeExecutableSchema } from '@graphql-tools/schema';
import express from 'express';
import http from 'http';
import cors from 'cors';
import fetch from 'node-fetch';
import { WebSocketServer } from 'ws';
import { useServer } from 'graphql-ws/lib/use/ws';
import { eventBus } from './event-bus.js';

// --- Configuration ---
const {
  SONGS_SERVICE_URL = 'http://localhost:3001/graphql',
  QUEUE_SERVICE_URL = 'http://localhost:3002/'
} = process.env;

const typeDefs = `
  type Song {
    id: ID!
    title: String!
    artist: String!
    duration: Int
  }

  type QueueItem {
    songId: String!
    position: Int!
    votes: Int!
    queuedAt: String
  }

  type QueueUpdateEvent {
    type: String!
    queue: [QueueItem!]!
    timestamp: String!
    user: String
    songId: String
  }

  type Query {
    songs: [Song!]!
    queue: [QueueItem!]!
  }

  type Mutation {
    queueSong(songId: String!): QueueItem
    upvoteSong(songId: String!): QueueItem
    downvoteSong(songId: String!): QueueItem
  }

  type Subscription {
    queueUpdated: QueueUpdateEvent
  }
`;

async function fetchGraphQL(url, query, variables) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables })
  });
  const json = await res.json();
  if (json.errors) {
    console.error('Downstream GraphQL errors:', json.errors);
    throw new Error('Downstream service error');
  }
  return json.data;
}

const resolvers = {
  Query: {
    songs: async () => {
      const data = await fetchGraphQL(SONGS_SERVICE_URL, '{ songs { id title artist duration } }');
      return data.songs;
    },
    queue: async () => {
      const data = await fetchGraphQL(QUEUE_SERVICE_URL, '{ queue { songId position votes queuedAt } }');
      return data.queue;
    }
  },
  Mutation: {
    queueSong: async (_, { songId }) => {
      const data = await fetchGraphQL(
        QUEUE_SERVICE_URL,
        'mutation QueueSongFromGateway($id: ID!) { queueSong(songId:$id){ songId, position, votes, queuedAt } }',
        { id: songId }
      );
      // The mutation now returns the new queue item, so we don't need to refetch.
      const newQueueItem = data.queueSong;

      const event = {
        type: 'SONG_QUEUED',
        // We no longer need to send the whole queue, just the song that was affected.
        // The client can use this to update its state or refetch if needed.
        songId,
        timestamp: new Date().toISOString(),
        user: 'anonymous'
      };
      eventBus.publishQueueUpdate(event);
      return newQueueItem;
    },
    upvoteSong: async (_, { songId }) => {
      const data = await fetchGraphQL(
        QUEUE_SERVICE_URL,
        'mutation UpvoteSongFromGateway($id: ID!) { upvoteSong(songId: $id) { songId, position, votes, queuedAt } }',
        { id: songId }
      );
      const updatedQueueItem = data.upvoteSong;

      const event = {
        type: 'SONG_UPVOTED',
        songId,
        timestamp: new Date().toISOString(),
        user: 'anonymous'
      };
      eventBus.publishQueueUpdate(event);
      return updatedQueueItem;
    },
    downvoteSong: async (_, { songId }) => {
      const data = await fetchGraphQL(
        QUEUE_SERVICE_URL,
        'mutation DownvoteSongFromGateway($id: ID!) { downvoteSong(songId: $id) { songId, position, votes, queuedAt } }',
        { id: songId }
      );
      const updatedQueueItem = data.downvoteSong;

      const event = {
        type: 'SONG_DOWNVOTED',
        songId,
        timestamp: new Date().toISOString(),
        user: 'anonymous'
      };
      eventBus.publishQueueUpdate(event);
      return updatedQueueItem;
    }
  },
  Subscription: {
    queueUpdated: {
      subscribe: () => eventBus.asyncIterator(),
    },
  },
};

const schema = makeExecutableSchema({ typeDefs, resolvers });

async function start() {
  const app = express();
  const httpServer = http.createServer(app);

  const server = new ApolloServer({
    schema,
    plugins: [
      ApolloServerPluginDrainHttpServer({ httpServer }),
    ]
  });

  await server.start();

  // Initialize the event bus (which will connect to Kafka if enabled)
  await eventBus.start();

  app.use(
    '/graphql',
    cors(),
    express.json(),
    expressMiddleware(server)
  );

  const wsServer = new WebSocketServer({
    server: httpServer,
    path: '/graphql',
  });

  useServer({ schema }, wsServer);

  app.get('/healthz', async (_req, res) => {
    const result = { status: 'ok', services: { songs: 'unknown', queue: 'unknown' } };
    try {
      const songs = await fetchGraphQL('http://localhost:3001/graphql', 'query HealthSongs { songs { id } }');
      result.services.songs = Array.isArray(songs.songs) ? 'up' : 'degraded';
    } catch { result.services.songs = 'down'; }
    try {
      const queue = await fetchGraphQL('http://localhost:3002/', 'query HealthQueue { queue { songId } }');
      result.services.queue = Array.isArray(queue.queue) ? 'up' : 'degraded';
    } catch { result.services.queue = 'down'; }
    res.json(result);
  });

  httpServer.listen(4000, () => {
    console.log('🎚️ mixer-console running:');
    console.log(`   HTTP: http://localhost:4000/graphql`);
    console.log(`   WS:   ws://localhost:4000/graphql`);
  });

  // --- Graceful Shutdown Logic ---
  const signals = ['SIGINT', 'SIGTERM'];
  signals.forEach((signal) => {
    process.on(signal, async () => {
      console.log(`\n👋 ${signal} received. Shutting down gracefully...`);
      
      // 1. Disconnect from Kafka
      await eventBus.shutdown();

      // 2. Close the HTTP server
      await new Promise(resolve => httpServer.close(resolve));
      
      console.log('✅ Server shut down complete.');
      process.exit(0);
    });
  });
}

start().catch(e => console.error('Startup error', e));