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
        Document existingDoc = collection.find(Filters.eq("_id", message.get("_id"))).first();
        if (existingDoc == null) {
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
            List<?> attributesList = (List<?>) existingDoc.get("attributes");
            Map<String, Object> attributeMap;
            // Then create the map, casting each attribute to Map<String, Object>
            try {
                attributeMap = attributesList.stream()
                        .map(attribute -> (Map<String, Object>) attribute)  // Cast each item
                        .collect(Collectors.toMap(
                                attribute -> (String) attribute.get("attrId"),  // Key mapper
                                attribute -> attribute                          // Value mapper
                        ));
            } catch (Exception e) {
                LOG.error("Error converting attributes to map: " + e.getMessage());
                return;
            }
            attributesList = (List<?>) message.get("attributes");

            // Get the new attributes list
            List<?> newAttributesList = (List<?>) message.get("attributes");

// Process each new attribute
            newAttributesList.stream()
                    .map(attribute -> (Map<String, Object>) attribute)
                    .forEach(newAttribute -> {
                        String attrid = (String) newAttribute.get("attrId");

                        if (attrid != null && attributeMap.containsKey(attrid)) {
                            Map<String, Object> existingAttribute = (Map<String, Object>) attributeMap.get(attrid);

                            // Compare tsCaptured values (assuming they are Long/Integer)
                            Date newTsCaptured = (Date) newAttribute.get("tsCaptured");
                            Date existingTsCaptured = (Date) existingAttribute.get("tsCaptured");

                            if (newTsCaptured != null && existingTsCaptured != null) {
                                // Convert to Long for comparison
                                Long newTs = newTsCaptured.getTime();
                                Long existingTs = existingTsCaptured.getTime();

                                // Replace it if the new timestamp is larger
                                if (newTs > existingTs) {
                                    attributeMap.put(attrid, newAttribute);
                                }
                            }
                        }
                    });

            List<Object> attributeList = attributeMap.values()
                    .stream()
                    .collect(Collectors.toList());

            newDoc.put("attributes", attributeList);
            WriteModel<Document> op = new ReplaceOneModel<>(new Document("_id", message.get("_id")), newDoc);
            SendToDatbase(op);
        }
    }
}
