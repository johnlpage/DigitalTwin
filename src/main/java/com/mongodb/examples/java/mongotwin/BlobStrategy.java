package com.mongodb.examples.java.mongotwin;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import org.bson.ByteBuf;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/*
This class still reads and writes the whole object from the database with all the issues in
ReadReplace but takes the object and stoes it as a Compressed Binary instead.
 */
public class BlobStrategy extends WriteStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(BlobStrategy.class);

    BlobStrategy(MongoClient mongoClient) {
        super();
        this.mongoClient = mongoClient;
        collection = mongoClient.getDatabase("digitwin").getCollection("twins");
    }

    private static Document getCompressedDocument(Map<String, Object> message) {
        // Now Compress i/ Convert to BSON bytes
        Document document = new Document(message);
        RawBsonDocument rawBsonDocument = RawBsonDocument.parse(document.toJson());
        ByteBuf byteBuffer = rawBsonDocument.getByteBuffer();
        byte[] bsonBytes = null;
        try {
            bsonBytes = BsonCompression.compressBsonBytes(byteBuffer.array());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Document compressedDocument = new Document("_id", message.get("_id")).append("payload", new Binary(bsonBytes));
        return compressedDocument;
    }

    private static Document getDecompressedDocument(Document compressedDocument) {
        // Extract the binary payload
        Binary binaryPayload = compressedDocument.get("payload", Binary.class);
        byte[] bsonBytes = null;
        try {
            bsonBytes = BsonCompression.decompressBsonBytes(binaryPayload.getData());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // Convert BSON bytes back to Document
        RawBsonDocument rawBsonDocument = new RawBsonDocument(bsonBytes);
        Document decompressedDocument = Document.parse(rawBsonDocument.toJson());

        return decompressedDocument;
    }

    public void WriteMessage(Map<String, Object> message) {
        // Fetch the Existing document
        Document existingDoc = null;

        //Shortcut for bulk loading
        if (!(Boolean) message.get("isNew")) {
            existingDoc = collection.find(Filters.eq("_id", message.get("_id"))).first();
            existingDoc = getDecompressedDocument(existingDoc);

        } else {
            message.remove("isNew");
        }

        if (existingDoc == null) {

            // Transform the incoming array to a Map
            List<?> fldList = (List<?>) message.get("fld");
            Map<String, Object> fldMap;
            // Then create the map, casting each fld to Map<String, Object>
            try {
                fldMap = fldList.stream()
                        .map(fld -> (Map<String, Object>) fld)  // Cast each item
                        .collect(Collectors.toMap(
                                fld -> (String) fld.get("fid"),  // Key mapper
                                fld -> fld                          // Value mapper
                        ));
            } catch (Exception e) {
                LOG.error("Error converting fld to map: " + e.getMessage());
                return;
            }
            message.put("fld", fldMap);

            Document compressedDocument = getCompressedDocument(message);

            WriteModel<Document> op = new InsertOneModel<>(compressedDocument);
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

            Map<String, Object> fldMap = (Map<String, Object>) existingDoc.get("fld");

            List<?> newfldList = (List<?>) message.get("fld");

            // Process each new fld
            newfldList.stream()
                    .map(fld -> (Map<String, Object>) fld)
                    .forEach(newfld -> {
                        String fldid = (String) newfld.get("fid");

                        if (fldid != null && fldMap.containsKey(fldid)) {
                            Map<String, Object> existingfld = (Map<String, Object>) fldMap.get(fldid);

                            // Compare ts values (assuming they are Long/Integer)
                            Date newts = (Date) newfld.get("ts");
                            Date existingts = (Date) existingfld.get("ts");

                            if (newts != null && existingts != null) {
                                // Convert to Long for comparison
                                Long newTs = newts.getTime();
                                Long existingTs = existingts.getTime();

                                // Replace it if the new timestamp is larger
                                if (newTs > existingTs) {
                                    fldMap.put(fldid, newfld);
                                } else {
                                    LOG.debug("Not replacing field " + fldid + " as it has a timestamp of " + existingTs);
                                }
                            }
                        }
                    });


            newDoc.put("fld", fldMap);
            Document compressedDocument = getCompressedDocument(newDoc);
            WriteModel<Document> op = new ReplaceOneModel<>(new Document("_id", compressedDocument.get("_id")), compressedDocument);
            SendToDatbase(op);
        }
    }
}
