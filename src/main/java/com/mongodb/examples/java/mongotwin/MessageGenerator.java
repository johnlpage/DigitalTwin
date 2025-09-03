package com.mongodb.examples.java.mongotwin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

/* This class generates messages coming from real world things (or possibly software)
    that contain multiple updates each with it's own timestamp - they may overlap
 */


public class MessageGenerator {
    final static int SHORT_STRING_LENGTH = 16;
    final static int TINY_STRING_LENGTH = 6;
    final static int OUT_OF_ORDER_EVERY = 10000; // One in 10,000 messages is out of order
    private static final Logger LOG = LoggerFactory.getLogger(MessageGenerator.class);
    // Format is Type, Cardinality, Name
    private final String[] topLevelFields = {
            "brand,String,20", "country,String,100", "version,Integer,10", "tripDataReason,String,100",
            "actuation,String,5", "fleetId,String,10000", "fleetProviderId,String,1000"
    };
    private final int nDevices;
    int NUM_TOTAL_fld = 60;
    int NUM_fld_PER_MESSAGE = 50;
    TechnicalStringGenerator tsg;
    Date streamStartTime = new Date();
    FastRCG rng = new FastRCG();
    Random realRNG = new Random();

    MessageGenerator(int nDevices, int changes, int totalAttributes) {
        this.nDevices = nDevices;
        tsg = new TechnicalStringGenerator();
        this.realRNG.setSeed(Thread.currentThread().getId());
        rng.setSeed(1); // Predictable and consistent results
        this.NUM_TOTAL_fld = totalAttributes;
        this.NUM_fld_PER_MESSAGE = changes;
    }

    //Some handy Hex
    String generateString(int seed, int length) {
        StringBuilder sb = new StringBuilder();
        rng.setSeed(seed); //Set see once and get a stream
        while (sb.length() < length + 1) {
            sb.append(String.format("%X ", rng.nextInt(0xFFFFF)));
        }
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    Map<String, Object> getField(int deviceId, Date now, int fld) {
        Map<String, Object> field = new HashMap<>();

        //timestamps - change between every reading

        field.put("nodeId", String.format("0x%010x", fld));
        field.put("timestampCarSent", now.getTime());
        field.put("timestampCarSentUTC", now.getTime());
        // Recorded a little earlier than send
        Date timeRecorded = new Date(now.getTime() - rng.nextInt(120000));
        field.put("tsCC", timeRecorded.getTime());
        field.put("tsCCUTC", timeRecorded.getTime());
        // Also mileage - we can relate that to timestamp
        int initialMileage = rng.nextInt(deviceId, 100000);

        int runningMins = (int) ((now.getTime() - streamStartTime.getTime()) / 60000);
        int recordiMins = (int) ((timeRecorded.getTime() - streamStartTime.getTime()) / 60000);
        // Some sort of device counter independant of time - like a car odometer, changes between some readings


        field.put("mileageCarCaptured", initialMileage + recordiMins);
        field.put("mileageCarSent", initialMileage + runningMins);

        String[] constFields = {"unit", "dataOwner", "textId", "picId", "actuation"};
        // Some things that dont change - but technically could ocattionally
        rng.setSeed(deviceId + fld);
        for (String cfname : constFields) {
            // 1 in 4 chance of null
            if (rng.nextInt(4) == 0) {
                field.put(cfname, null);
            } else {
                int stringId = rng.nextInt(100);
                field.put(cfname, tsg.generateString(stringId, SHORT_STRING_LENGTH));
            }

        }
        field.put("expiresAt", null);
        // The value itself will sometimes vary constantly and other times vary occasioanlly

        if (fld % 2 == 0) {
            // Even ones change always

            int seed = Math.toIntExact((System.currentTimeMillis() % 0xFFFFFF) + deviceId + fld);
            field.put("value", rng.nextInt(seed, 1000000));
        } else {
            //Even ones change every 5 mins
            int seed = Math.toIntExact((System.currentTimeMillis() / 300000) + deviceId + fld);
            field.put("value", rng.nextInt(seed, 1000000));
        }

        field.put("dataKeyId", null);
        field.put("initialisationVector", null);
        field.put("isConfidential", false);
        return field;
    }

    Map<String, Object> getMessage() {
        return getMessage(false, null);
    }

    //Can use for data generation if we ask

    Map<String, Object> getMessage(boolean full, Integer deviceId) {
        HashMap<String, Object> message = new HashMap<>();
        if (deviceId == null) {
            deviceId = realRNG.nextInt(nDevices);
        }
        message.put("isNew", full);
        message.put("_id", "V_" + deviceId);
        Date now = new Date(); // Current Time
        // Occasioanlly we get a message thats not from now, it's from a while ago
        if (rng.nextInt((int) (System.currentTimeMillis() + deviceId), OUT_OF_ORDER_EVERY) == 0) {
            now = new Date(now.getTime() - rng.nextInt(120000));
        }
        message.put("timestampReceived", now);

        // For now we can keep these constant, but code will work if we change them
        int tlc = 0;
        for (String topLevelField : topLevelFields) {
            String[] parts = topLevelField.split(",");
            int cardinality = Integer.parseInt(parts[2]);
            Object value = switch (parts[1]) {
                case "String" -> tsg.generateString(rng.nextInt(deviceId + tlc++, cardinality), TINY_STRING_LENGTH);
                case "Integer" -> rng.nextInt(deviceId + tlc++, cardinality);
                default -> null;
            };
            message.put(parts[0], value);
            List<Integer> fieldsToPopulate = new ArrayList<>();
            List<Map<String, Object>> fld = new ArrayList<Map<String, Object>>();

            if (!full) {
                Set<Integer> fieldSubset = new HashSet();
                //Pick a non duplicate subset - this may be inefficient - TODO
                while (fieldSubset.size() < NUM_fld_PER_MESSAGE) {
                    int fno = rng.nextInt(NUM_TOTAL_fld);
                    if (!fieldSubset.contains(fno)) {
                        fieldSubset.add(fno);
                    }
                }
                fieldsToPopulate = new ArrayList<>(fieldSubset);
            } else {
                fieldsToPopulate = IntStream.range(0, NUM_TOTAL_fld).boxed().toList();
            }

            for (int f : fieldsToPopulate) {
                fld.add(getField(deviceId, now, f));
            }
            message.put("e", fld);

        }

        return message;
    }
}
