package com.mongodb.examples.java.mongotwin;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


/* This version puts all the work on the server and does not read back the existing document.
to the client - it also minimised oplog content to only include specific values that change
 */
public class ServerSideStrategy extends WriteStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ServerSideStrategy.class);

    ServerSideStrategy(MongoClient mongoClient) {
        super();
        this.mongoClient = mongoClient;
        collection = mongoClient.getDatabase("digitwin").getCollection("twins");
    }

    public void WriteMessage(Map<String, Object> message) {

        Bson query = Filters.eq("_id", message.get("_id"));

        // For the top level fields we will simply set them - there is no
        // time element - we dont xpecta  change but we can support it
        // Copy over the top level fields
        Map<String, Object> afterValues = new HashMap<>();

        for (String topLevelField : message.keySet()) {
            Object value = message.get(topLevelField);
            if (value instanceof String || value instanceof Integer || value instanceof Date) {
                afterValues.put(topLevelField, message.get(topLevelField));
            }
        }

        //For Each value in each element in the fld array we want to conditionally set it

        List<?> fldList = (List<?>) message.get("e");
        for (Object fld : fldList) {
            Map<String, Object> fldMap = (Map<String, Object>) fld;
            String fldid = (String) fldMap.get("nodeId");
            Long messageTimestamp = (Long) fldMap.get("tsCC");
            for (String key : fldMap.keySet()) {
                if (!key.equals(fldid)) {
                    // So Set this field (in the server) to either the latest value or the existing value
                    // Depending on whether the existing ts is greater or less than the new ts
                    String path = String.format("e.%s.%s", fldid, key);
                    String existingTimestamp = String.format("$e.%s.tsCC", fldid);
                    Object newValue = fldMap.get(key);
                    Document isNewer = new Document("$gt", Arrays.asList(messageTimestamp, existingTimestamp));
                    Document conditional = new Document("$cond", Arrays.asList(isNewer, newValue, "$" + path));
                    afterValues.put(path, conditional);
                }
            }
        }
        LOG.info(new Document("$set", afterValues).toJson());

        List<Document> updateSteps = new ArrayList<>();
        UpdateOptions updateOptions = new UpdateOptions().upsert(true);
        updateSteps.add(new Document("$set", afterValues));
        WriteModel<Document> op = new UpdateOneModel<>(query, updateSteps, updateOptions);
        SendToDatbase(op);
    }
}

