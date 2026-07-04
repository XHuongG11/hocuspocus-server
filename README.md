# hocuspocus-server

WebSocket server providing real-time collaborative editing of roadmap documents for MeMap.

## Overview

A Node.js/TypeScript server built on `@hocuspocus/server` that enables multiple users to collaboratively edit a roadmap simultaneously using Yjs CRDT. On document load it fetches the roadmap (nodes, edges, metadata) from MongoDB. On every save it publishes a full snapshot of the current Y.Doc state to a Redis Stream (`roadmap-document-updates`), which roadmap-service consumes to persist changes. Also acts as a WebRTC signalling relay — handling offer/answer/ICE candidate exchange for peer-to-peer audio/video presence.

## Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Node.js | 18+ | Runtime |
| TypeScript | 5.x | Language |
| @hocuspocus/server | 3.x | WebSocket server with Y.Doc lifecycle hooks |
| Yjs | 13.x | CRDT document model (shared maps for nodes, edges, meta) |
| MongoDB (mongodb driver) | - | Load roadmap on document open; joins `RoadMap` + `RoadMapCategory` |
| Redis (ioredis) | - | Publish `DOCUMENT_UPDATED` events to `roadmap-document-updates` stream |

## Port & Routes

- WebSocket: `:1234` (single endpoint — document name = roadmap ID)

Clients connect with `new HocuspocusProvider({ url: 'ws://localhost:1234', name: '<roadmapId>' })`.

## Key Features

- **Document load**: `onLoadDocument` aggregates the `RoadMap` collection with `RoadMapCategory` via `$lookup`, then populates Y.Maps: `nodes` (keyed by `nodeId`), `edges` (keyed by `edgeId`), `meta`, and `roadmapInfo`.
- **Document persistence**: `onStoreDocument` extracts current state from Y.Maps and publishes a `DOCUMENT_UPDATED` event to the Redis Stream with full node/edge/roadmapInfo payloads. roadmap-service consumes this stream to write back to MongoDB.
- **String vs ObjectId ID handling**: Automatically detects whether the roadmap `_id` is stored as a plain string or BSON `ObjectId` and queries accordingly.
- **Room management**: Tracks connected clients per document in an in-memory `rooms` Map.
- **WebRTC signalling**: `onStateless` handles `join`, `offer`, `answer`, `candidate`, and `user_exit` messages, relaying SDP and ICE candidates between peers for video/audio presence features.
- **Presence broadcast**: On join, all current room members receive a `room_users` list; on disconnect, remaining members receive a `user_exit` notification.
- **Redis retry**: ioredis configured with exponential back-off reconnect strategy (max 5 s delay).

## Running Locally

```bash
cd hocuspocus-server
yarn dev      # nodemon + ts-node (watch mode)
yarn build    # compile TypeScript to dist/
yarn start    # node dist/server.js
```

Requires a reachable MongoDB instance with the `roadmap_service` database and a Redis instance.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `HOCUSPOCUS_PORT` | `1234` | WebSocket server port |
| `MONGODB_URL` | `mongodb://roadmap_user:roadmap_pass123@localhost:27017?authSource=roadmap_service` | MongoDB connection URI |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6382` | Redis port |
| `REDIS_STREAM_KEY` | `roadmap-document-updates` | Redis Stream key for publishing document snapshots |
