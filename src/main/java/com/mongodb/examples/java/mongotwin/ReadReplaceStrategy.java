package com.mongodb.examples.java.mongotwin;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/* This class updates by doing what most users assume you do with MongoDB - when it wants to merge
in some data conditionally - it reads the existing document into the client, modifies it using Java
then overwrites it in the database. This is what happens if you use Spring and save() for example.

This is exceptionally inefficient as you need to read each record to update it ( Paying read costs and network egress
costs)

You put the whole new version into the transaction replication log (oplog) so it needs to be copied to the
secondaries and applied - even if just one value changed

If using point in time backups, then the whole document ends up in the oplog.

We are still being as smart as we can about network hops and ammortising durability costs by using bulk write
operations rather than individual network calls although in Spring Data this is usually overlooked too
 */
public class ReadReplaceStrategy extends WriteStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(ReadReplaceStrategy.class);

    ReadReplaceStrategy(MongoClient mongoClient) {
        super();
        this.mongoClient = mongoClient;
        collection = mongoClient.getDatabase("digitwin").getCollection("twins");
    }

    public void WriteMessage(Map<String, Object> message) {
        // Fetch the Existing document
        Document existingDoc = null;

        //Shortcut for bulk loading
        if (!(Boolean) message.get("isNew")) {
            existingDoc = collection.find(Filters.eq("_id", message.get("_id"))).first();
        } else {
            message.remove("isNew");
        }

        if (existingDoc == null) {

            // Transform the incoming array to a Map
            List<?> fldList = (List<?>) message.get("e");
            Map<String, Object> fldMap;
            // Then create the map, casting each fld to Map<String, Object>
            try {
                fldMap = fldList.stream()
                        .map(fld -> (Map<String, Object>) fld)  // Cast each item
                        .collect(Collectors.toMap(
                                fld -> (String) fld.get("nodeId"),  // Key mapper
                                fld -> fld                          // Value mapper
                        ));
            } catch (Exception e) {
                LOG.error("Error converting fld to map: " + e.getMessage());
                return;
            }
            message.put("e", fldMap);
            WriteModel<Document> op = new InsertOneModel<>(new Document(message));
            SendToDatbase(op);
            return;
        } else {
            Document newDoc = new Document(message);
            // Copy over the top level fields
            for (String topLevelField : message.keySet()) {
                Object value = message.get(topLevelField);
                if (value instanceof String || value instanceof Integer || value instanceof Date) {
                    newDoc.put(topLevelField, message.get(topLevelField));
                }
            }
            Map<String, Object> fldMap = null;
            try {
                fldMap = (Map<String, Object>) existingDoc.get("e");
            } catch (Exception e) {
                LOG.error("Error converting fld to map: " + e.getMessage());
            }
            List<?> newfldList = (List<?>) message.get("e");

            // Process each new fld
            Map<String, Object> finalFldMap = fldMap;
            newfldList.stream()
                    .map(fld -> (Map<String, Object>) fld)
                    .forEach(newfld -> {
                        String fldid = (String) newfld.get("nodeId");

                        if (fldid != null && finalFldMap.containsKey(fldid)) {
                            Map<String, Object> existingfld = (Map<String, Object>) finalFldMap.get(fldid);

                            // Compare ts values (assuming they are Long/Integer)
                            Long newTs = (Long) newfld.get("tsCC");
                            Long existingTs = (Long) existingfld.get("tsCC");

                            if (newTs != null && existingTs != null) {

                                // Replace it if the new timestamp is larger
                                if (newTs > existingTs) {
                                    finalFldMap.put(fldid, newfld);
                                } else {
                                    LOG.debug("Not replacing fld " + fldid + " as it has a timestamp of " + existingTs);
                                }
                            }
                        }
                    });


            newDoc.put("e", fldMap);
            WriteModel<Document> op = new ReplaceOneModel<>(new Document("_id", message.get("_id")), newDoc);
            SendToDatbase(op);
        }
    }
}
