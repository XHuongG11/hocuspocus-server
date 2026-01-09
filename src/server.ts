import { Server } from "@hocuspocus/server";
import { MongoClient } from "mongodb";

async function start() {
  const mongo = new MongoClient(
    "mongodb://roadmap_user:roadmap_pass123@84.247.150.108:27017?authSource=roadmap_service"
  );
  try {
    await mongo.connect();
    console.log("Mongo connected OK");
  } catch (e) {
    console.error("Mongo connect error:", e);
  }
  const db = mongo.db("roadmap_service");

  const server = new Server({
    async onLoadDocument({ documentName, document }) {
      // documentName = roadmapId
      // const roadmap = await db.collection("RoadMap").findOne({
      //   _id: { $eq: documentName } as any,
      // });

      const result = await db
        .collection("RoadMap")
        .aggregate([
          {
            $match: {
              _id: documentName,
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

      // console.log("Loading roadmap:", documentName, roadmap);

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
      // console.log("Loaded roadmap into document:", document.toJSON());
    },
  });

  server.listen(1234);
  console.log("Hocuspocus server is running on port 1234");
}

start();
