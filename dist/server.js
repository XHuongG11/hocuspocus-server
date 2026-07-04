"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const server_1 = require("@hocuspocus/server");
const mongodb_1 = require("mongodb");
const ioredis_1 = __importDefault(require("ioredis"));
const rooms = new Map();
const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6382", 10);
const REDIS_STREAM_KEY = process.env.REDIS_STREAM_KEY || "roadmap-document-updates";
const MONGODB_URL = process.env.MONGODB_URL ||
    "mongodb://roadmap_user:roadmap_pass123@84.247.150.108:27017?authSource=roadmap_service";
const HOCUSPOCUS_PORT = parseInt(process.env.HOCUSPOCUS_PORT || "1234", 10);
async function start() {
    // --- MongoDB connection ---
    const mongo = new mongodb_1.MongoClient(MONGODB_URL);
    try {
        await mongo.connect();
        console.log("Mongo connected OK");
    }
    catch (e) {
        console.error("Mongo connect error:", e);
        process.exit(1);
    }
    const db = mongo.db("roadmap_service");
    // --- Redis connection ---
    const redis = new ioredis_1.default({
        host: REDIS_HOST,
        port: REDIS_PORT,
        retryStrategy(times) {
            const delay = Math.min(times * 500, 5000);
            console.warn(`Redis reconnect attempt #${times}, retrying in ${delay}ms...`);
            return delay;
        },
        maxRetriesPerRequest: 3,
    });
    redis.on("connect", () => {
        console.log(`Redis connected OK (${REDIS_HOST}:${REDIS_PORT})`);
    });
    redis.on("error", (err) => {
        console.error("Redis connection error:", err.message);
    });
    const server = new server_1.Server({
        async onConnect({ documentName }) {
            if (!rooms.has(documentName)) {
                rooms.set(documentName, new Map());
            }
        },
        async onDisconnect({ documentName, socketId }) {
            const room = rooms.get(documentName);
            if (!room)
                return;
            const myId = socketId;
            if (!myId)
                return;
            room.delete(myId);
            if (room.size === 0) {
                rooms.delete(documentName);
                return;
            }
            const exitMsg = JSON.stringify({ type: "user_exit", id: myId });
            room.forEach(({ connection: c }) => c.sendStateless(exitMsg));
        },
        async onStateless({ payload, connection, documentName }) {
            let msg;
            try {
                msg = JSON.parse(payload);
            }
            catch (e) {
                console.error("[onStateless] Malformed payload, ignoring:", payload);
                return;
            }
            const room = rooms.get(documentName);
            if (!room)
                return;
            const myId = connection.socketId;
            if (msg.type === "join") {
                room.set(myId, { connection, name: msg.name });
                connection.sendStateless(JSON.stringify({ type: "connected", id: myId }));
                const users = Array.from(room.entries()).map(([id, u]) => ({ id, name: u.name }));
                const usersMsg = JSON.stringify({ type: "room_users", users });
                room.forEach(({ connection: c }) => c.sendStateless(usersMsg));
            }
            else if (msg.type === "offer") {
                room.get(msg.to)?.connection.sendStateless(JSON.stringify({ type: "getOffer", from: myId, sdp: msg.sdp }));
            }
            else if (msg.type === "answer") {
                room.get(msg.to)?.connection.sendStateless(JSON.stringify({ type: "getAnswer", from: myId, sdp: msg.sdp }));
            }
            else if (msg.type === "candidate") {
                room.get(msg.to)?.connection.sendStateless(JSON.stringify({ type: "getCandidate", from: myId, candidate: msg.candidate }));
            }
            else if (msg.type === "user_exit") {
                room.delete(myId);
                const exitMsg = JSON.stringify({ type: "user_exit", id: myId });
                room.forEach(({ connection: c }) => c.sendStateless(exitMsg));
            }
        },
        async onLoadDocument({ documentName, document }) {
            console.log(`[onLoadDocument] Loading document: "${documentName}"`);
            // Diagnostic: check what _id type is actually stored in MongoDB
            const sampleDoc = await db
                .collection("RoadMap")
                .findOne({ _id: documentName });
            const sampleDocObj = await db
                .collection("RoadMap")
                .findOne({ _id: new mongodb_1.ObjectId(documentName) });
            console.log(`[onLoadDocument] findOne(string): ${!!sampleDoc}, findOne(ObjectId): ${!!sampleDocObj}`);
            // Try string first (Spring @MongoId String), fall back to ObjectId
            const matchId = sampleDoc
                ? documentName
                : new mongodb_1.ObjectId(documentName);
            const result = await db
                .collection("RoadMap")
                .aggregate([
                {
                    $match: {
                        _id: matchId,
                    },
                },
                {
                    $lookup: {
                        from: "RoadMapCategory",
                        let: { categoryId: "$category.$id" },
                        pipeline: [
                            {
                                $match: {
                                    $expr: { $eq: ["$_id", "$$categoryId"] },
                                },
                            },
                            {
                                $project: {
                                    _id: 0,
                                    id: "$_id",
                                    name: 1,
                                    description: 1,
                                },
                            },
                        ],
                        as: "category",
                    },
                },
                {
                    $unwind: {
                        path: "$category",
                        preserveNullAndEmptyArrays: true,
                    },
                },
                {
                    $project: {
                        created_date: 0,
                        updated_date: 0,
                        is_deleted: 0,
                        _class: 0,
                    },
                },
            ])
                .toArray();
            const roadmap = result[0];
            console.log(`[onLoadDocument] Query result count: ${result.length}, roadmap found: ${!!roadmap}`);
            if (roadmap) {
                console.log(`[onLoadDocument] Roadmap name: "${roadmap.name}", nodes: ${roadmap.nodes?.length}, edges: ${roadmap.edges?.length}`);
            }
            if (!roadmap)
                return;
            const nodesMap = document.getMap("nodes");
            const edgesMap = document.getMap("edges");
            const metaMap = document.getMap("meta");
            const roadmapInfo = document.getMap("roadmapInfo");
            // tránh ghi đè khi đã có dữ liệu
            // if (nodesMap.size > 0) return;
            document.transact(() => {
                metaMap.set("name", roadmap.name);
                // console.log("roadmap.nodes:", roadmap.nodes);
                (roadmap.nodes ?? []).forEach((node) => {
                    nodesMap.set(node.nodeId, node);
                });
                // console.log("roadmap.edges:", roadmap.edges);
                (roadmap.edges ?? []).forEach((edge) => {
                    edgesMap.set(edge.edgeId, edge);
                });
                // category is optional (e.g. AI-generated roadmaps are uncategorized);
                // guard against null so hydration never aborts and leaves a blank editor.
                roadmapInfo.set("roadmapInfo", {
                    name: roadmap.name,
                    description: roadmap.description,
                    category: roadmap.category
                        ? {
                            id: roadmap.category.id,
                            name: roadmap.category.name,
                            description: roadmap.category.description,
                        }
                        : null,
                });
            });
            console.log("Loaded roadmap into document:", document.toJSON());
        },
        async onStoreDocument({ documentName, document }) {
            const roadmapId = documentName;
            try {
                const nodesMap = document.getMap("nodes");
                const edgesMap = document.getMap("edges");
                const roadmapInfoMap = document.getMap("roadmapInfo");
                // Extract current state from Y.Maps
                const nodes = [];
                nodesMap.forEach((value, key) => {
                    nodes.push(value);
                });
                const edges = [];
                edgesMap.forEach((value, key) => {
                    edges.push(value);
                });
                const roadmapInfo = roadmapInfoMap.get("roadmapInfo") || {};
                // Publish full snapshot to Redis Stream
                await redis.xadd(REDIS_STREAM_KEY, "*", "eventType", "DOCUMENT_UPDATED", "roadmapId", roadmapId, "nodes", JSON.stringify(nodes), "edges", JSON.stringify(edges), "roadmapInfo", JSON.stringify(roadmapInfo), "timestamp", new Date().toISOString());
                console.log(`📤 Published DOCUMENT_UPDATED to Redis Stream for roadmap: ${roadmapId} (${nodes.length} nodes, ${edges.length} edges)`);
            }
            catch (err) {
                console.error(`❌ Failed to publish to Redis Stream for roadmap ${roadmapId}:`, err.message);
                // Don't throw — Y.Doc remains the source of truth.
                // Next onStoreDocument will retry with the latest state.
            }
        },
    });
    server.listen(HOCUSPOCUS_PORT);
    console.log(`Hocuspocus server is running on port ${HOCUSPOCUS_PORT}`);
}
start();
