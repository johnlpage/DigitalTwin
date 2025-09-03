package com.mongodb.examples.java.mongotwin;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class BsonCompression {

    // Compress BSON bytes using zlib (Deflater)
    public static byte[] compressBsonBytes(byte[] bsonBytes) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(bsonBytes);
        deflater.finish();

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer);
                baos.write(buffer, 0, count);
            }
            return baos.toByteArray();
        } finally {
            deflater.end();
        }
    }

    // Decompress zlib-compressed bytes back to BSON
    public static byte[] decompressBsonBytes(byte[] compressedBytes) throws IOException {
        Inflater inflater = new Inflater();
        inflater.setInput(compressedBytes);

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                try {
                    int count = inflater.inflate(buffer);
                    baos.write(buffer, 0, count);
                } catch (DataFormatException e) {
                    throw new IOException("Invalid compressed data format", e);
                }
            }
            return baos.toByteArray();
        } finally {
            inflater.end();
        }
    }
}
