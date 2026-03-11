import "dotenv/config";
import { Server } from "@hocuspocus/server";
import { MongoClient, ObjectId } from "mongodb";
import Redis from "ioredis";

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6382", 10);
const REDIS_STREAM_KEY =
  process.env.REDIS_STREAM_KEY || "roadmap-document-updates";
const MONGODB_URL =
  process.env.MONGODB_URL ||
  "mongodb://roadmap_user:roadmap_pass123@84.247.150.108:27017?authSource=roadmap_service";
const HOCUSPOCUS_PORT = parseInt(process.env.HOCUSPOCUS_PORT || "1234", 10);

async function start() {
  // --- MongoDB connection ---
  const mongo = new MongoClient(MONGODB_URL);
  try {
    await mongo.connect();
    console.log("Mongo connected OK");
  } catch (e) {
    console.error("Mongo connect error:", e);
  }
  const db = mongo.db("roadmap_service");

  // --- Redis connection ---
  const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    retryStrategy(times) {
      const delay = Math.min(times * 500, 5000);
      console.warn(
        `Redis reconnect attempt #${times}, retrying in ${delay}ms...`,
      );
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

  const server = new Server({
    async onLoadDocument({ documentName, document }) {
      console.log(`[onLoadDocument] Loading document: "${documentName}"`);

      // Diagnostic: check what _id type is actually stored in MongoDB
      const sampleDoc = await db
        .collection("RoadMap")
        .findOne({ _id: documentName as any });
      const sampleDocObj = await db
        .collection("RoadMap")
        .findOne({ _id: new ObjectId(documentName) as any });
      console.log(
        `[onLoadDocument] findOne(string): ${!!sampleDoc}, findOne(ObjectId): ${!!sampleDocObj}`,
      );

      // Try string first (Spring @MongoId String), fall back to ObjectId
      const matchId: any = sampleDoc
        ? documentName
        : new ObjectId(documentName);

      const result = await db
        .collection("RoadMap")
        .aggregate([
          {
            $match: {
              _id: matchId as any,
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

      console.log(
        `[onLoadDocument] Query result count: ${result.length}, roadmap found: ${!!roadmap}`,
      );
      if (roadmap) {
        console.log(
          `[onLoadDocument] Roadmap name: "${roadmap.name}", nodes: ${roadmap.nodes?.length}, edges: ${roadmap.edges?.length}`,
        );
      }

      if (!roadmap) return;

      const nodesMap = document.getMap("nodes");
      const edgesMap = document.getMap("edges");
      const metaMap = document.getMap("meta");
      const roadmapInfo = document.getMap("roadmapInfo");

      // tránh ghi đè khi đã có dữ liệu
      // if (nodesMap.size > 0) return;

      document.transact(() => {
        metaMap.set("name", roadmap.name);

        // console.log("roadmap.nodes:", roadmap.nodes);
        roadmap.nodes.forEach((node: any) => {
          nodesMap.set(node.nodeId, node);
        });

        // console.log("roadmap.edges:", roadmap.edges);

        roadmap.edges.forEach((edge: any) => {
          edgesMap.set(edge.edgeId, edge);
        });

        roadmapInfo.set("roadmapInfo", {
          name: roadmap.name,
          description: roadmap.description,
          category: {
            id: roadmap.category.id,
            name: roadmap.category.name,
            description: roadmap.category.description,
          },
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
        const nodes: any[] = [];
        nodesMap.forEach((value, key) => {
          nodes.push(value);
        });

        const edges: any[] = [];
        edgesMap.forEach((value, key) => {
          edges.push(value);
        });

        const roadmapInfo = roadmapInfoMap.get("roadmapInfo") || {};

        // Publish full snapshot to Redis Stream
        await redis.xadd(
          REDIS_STREAM_KEY,
          "*",
          "eventType",
          "DOCUMENT_UPDATED",
          "roadmapId",
          roadmapId,
          "nodes",
          JSON.stringify(nodes),
          "edges",
          JSON.stringify(edges),
          "roadmapInfo",
          JSON.stringify(roadmapInfo),
          "timestamp",
          new Date().toISOString(),
        );

        console.log(
          `📤 Published DOCUMENT_UPDATED to Redis Stream for roadmap: ${roadmapId} (${nodes.length} nodes, ${edges.length} edges)`,
        );
      } catch (err: any) {
        console.error(
          `❌ Failed to publish to Redis Stream for roadmap ${roadmapId}:`,
          err.message,
        );
        // Don't throw — Y.Doc remains the source of truth.
        // Next onStoreDocument will retry with the latest state.
      }
    },
  });

  server.listen(HOCUSPOCUS_PORT);
  console.log(`Hocuspocus server is running on port ${HOCUSPOCUS_PORT}`);
}

start();
