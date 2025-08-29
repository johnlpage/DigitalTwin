package com.mongodb.examples.java.mongotwin;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WriteStrategy implements AutoCloseable {

    final int BULK_SIZE = 100;
    MongoClient mongoClient;
    MongoCollection<Document> collection;
    List<WriteModel<Document>> bulkOps = null;

    WriteStrategy() {
        bulkOps = new ArrayList<WriteModel<Document>>();
    }

    public void WriteMessage(Map<String, Object> message) {
    }

    void SendToDatbase(WriteModel<Document> op) {

        bulkOps.add(op);
        if (bulkOps.size() >= BULK_SIZE) {
            collection.bulkWrite(bulkOps);
            bulkOps.clear();
        }
    }

    @Override
    public void close() throws Exception {
        collection = mongoClient.getDatabase("digitwin").getCollection("twins");
        if (!bulkOps.isEmpty()) {
            collection.bulkWrite(bulkOps);
        }
    }
}
