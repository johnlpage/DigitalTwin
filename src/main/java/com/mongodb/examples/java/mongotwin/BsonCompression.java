package com.mongodb.examples.java.mongotwin;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class BsonCompression {

    // Compress BSON bytes using GZIP
    public static byte[] compressBsonBytes(byte[] bsonBytes) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(bsonBytes);
        }
        return baos.toByteArray();
    }

    // Decompress GZIP compressed bytes back to BSON
    public static byte[] decompressBsonBytes(byte[] compressedBytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(compressedBytes);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (GZIPInputStream gzipIn = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipIn.read(buffer)) != -1) {
                baos.write(buffer, 0, len);
            }
        }
        return baos.toByteArray();
    }
}
