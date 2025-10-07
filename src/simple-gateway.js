import express from 'express';
import { createServer } from 'http';
import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import { PubSub } from 'graphql-subscriptions';
import { eventBus } from './event-bus.js';

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
      const data = await fetchGraphQL(
        'http://localhost:3001/graphql',
        'query GetSongsForGateway { songs { id title artist duration } }'
      );
      return data.songs;
    },
    queue: async () => {
      const data = await fetchGraphQL(
        'http://localhost:3002/',
        'query GetQueueForGateway { queue { songId position votes queuedAt } }'
      );
      return data.queue;
    }
  },
  Mutation: {
    queueSong: async (_, { songId }) => {
      await fetchGraphQL(
        'http://localhost:3002/',
        'mutation QueueSongFromGateway($id: ID!) { queueSong(songId:$id){ songId } }',
        { id: songId }
      );
      const queue = await fetchGraphQL('http://localhost:3002/', 'query GetQueueForGateway { queue { songId position votes queuedAt } }');
      const event = {
        type: 'SONG_QUEUED',
        queue: queue.queue,
        timestamp: new Date().toISOString(),
        songId,
        user: 'anonymous'
      };
      eventBus.publishQueueUpdate(event);
      return queue.queue.find(item => item.songId === songId);
    },
    upvoteSong: async (_, { songId }) => {
      await fetchGraphQL(
        'http://localhost:3002/',
        'mutation UpvoteSongFromGateway($id: ID!) { upvoteSong(songId: $id) { songId } }',
        { id: songId }
      );
      const queue = await fetchGraphQL('http://localhost:3002/', 'query GetQueueForGateway { queue { songId position votes queuedAt } }');
      const event = {
        type: 'SONG_UPVOTED',
        queue: queue.queue,
        timestamp: new Date().toISOString(),
        songId,
        user: 'anonymous'
      };
      eventBus.publishQueueUpdate(event);
      return queue.queue.find(item => item.songId === songId);
    },
    downvoteSong: async (_, { songId }) => {
      await fetchGraphQL(
        'http://localhost:3002/',
        'mutation DownvoteSongFromGateway($id: ID!) { downvoteSong(songId: $id) { songId } }',
        { id: songId }
      );
      const queue = await fetchGraphQL('http://localhost:3002/', 'query GetQueueForGateway { queue { songId position votes queuedAt } }');
      const event = {
        type: 'SONG_DOWNVOTED',
        queue: queue.queue,
        timestamp: new Date().toISOString(),
        songId,
        user: 'anonymous'
      };
      eventBus.publishQueueUpdate(event);
      return queue.queue.find(item => item.songId === songId);
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
  const httpServer = createServer(app);

  const apollo = new ApolloServer({
    schema,
    plugins: [
      ApolloServerPluginDrainHttpServer({ httpServer }),
    ]
  });

  await apollo.start();
  app.use('/graphql', cors(), express.json(), expressMiddleware(apollo));

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
    console.log('   HTTP: http://localhost:4000/graphql');
    console.log('   WS:   ws://localhost:4000/graphql');
  });
}

start().catch(e => console.error('Startup error', e));