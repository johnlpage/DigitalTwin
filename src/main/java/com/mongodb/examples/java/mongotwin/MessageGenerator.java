package com.mongodb.examples.java.mongotwin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.IntStream;

/* This class generates messages coming from real world things (or possibly software)
    that contain multiple updates each with it's own timestamp - they may overlap
 */


public class MessageGenerator {

    final static int NUM_TOTAL_ATTRIBUTES = 300;
    final static int NUM_ATTRIBUTES_PER_MESSAGE = 50;
    final static int NUM_DEVICES = 10000;
    final static int SHORT_STRING_LENGTH = 16;
    final static int TINY_STRING_LENGTH = 6;
    final static int OUT_OF_ORDER_EVERY = 10000; // One in 10,000 messages is out of order
    private static final Logger LOG = LoggerFactory.getLogger(MessageGenerator.class);
    // Format is Type, Cardinality, Name
    private final String[] topLevelFields = {
            "brand,String,20", "country,String,100", "version,Integer,10", "tripDataReason,String,100",
            "actuation,String,5", "fleetId,String,10000", "fleetProviderId,String,1000"
    };

    Date streamStartTime = new Date();
    FastRCG rng = new FastRCG();

    MessageGenerator() {
        rng.setSeed(1); // Predictable and consistent results
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

    Map<String, Object> getAttribute(int deviceId, Date now, int attrId) {
        Map<String, Object> attr = new HashMap<>();

        //timestamps - change between every reading
        attr.put("attrId", String.format("A_%03x", attrId));
        attr.put("tsSent", now);
        // Recorded a little earlier than send
        Date timeRecorded = new Date(now.getTime() - rng.nextInt(120000));
        attr.put("tsCaptured", timeRecorded);

        // Also mileage - we can relate that to timestamp
        int initialMileage = rng.nextInt(deviceId, 100000);

        int runningMins = (int) ((now.getTime() - streamStartTime.getTime()) / 60000);
        int recordiMins = (int) ((timeRecorded.getTime() - streamStartTime.getTime()) / 60000);
        // Some sort of device counter independant of time - like a car odometer, changes between some readings


        attr.put("mileageRecorded", initialMileage + recordiMins);
        attr.put("mileageSent", initialMileage + runningMins);

        String[] constFields = {"unit", "dataOwner", "textId", "picId"};
        // Some things that dont change - but technically could ocattionally
        rng.setSeed(deviceId + attrId);
        for (String cfname : constFields) {
            int stringId = rng.nextInt(100);
            attr.put(cfname, generateString(stringId, TINY_STRING_LENGTH));
        }

        // The value itself will sometimes vary constantly and other times vary occasioanlly

        if (attrId % 2 == 0) {
            // Even ones change always
            attr.put("value", rng.nextInt((int) System.currentTimeMillis() + deviceId + attrId, 1000000));
        } else {
            //Even ones change every 5 mins
            attr.put("value", rng.nextInt((int) (System.currentTimeMillis() / 300000) + deviceId + attrId, 1000000));
        }

        return attr;
    }

    Map<String, Object> getMessage() {
        return getMessage(false, null);
    }

    //Can use for data generation if we ask

    Map<String, Object> getMessage(boolean full, Integer deviceId) {
        HashMap<String, Object> message = new HashMap<>();
        if (deviceId == null) {
            deviceId = rng.nextInt(NUM_DEVICES);
        }

        message.put("_id", String.format("V_%08d", deviceId));

        Date now = new Date(); // Current Time


        // Occasioanlly we get a message thats not from now, it's from a while ago
        if (rng.nextInt((int) (System.currentTimeMillis() + deviceId), OUT_OF_ORDER_EVERY) == 0) {
            now = new Date(now.getTime() - rng.nextInt(120000));
        }
        message.put("timestamp", now);

        // For now we can keep these constant, but code will work if we change them

        for (String topLevelField : topLevelFields) {
            String[] parts = topLevelField.split(",");
            int cardinality = Integer.parseInt(parts[2]);
            Object value = switch (parts[1]) {
                case "String" -> generateString(rng.nextInt(deviceId, cardinality), SHORT_STRING_LENGTH);
                case "Integer" -> rng.nextInt(deviceId, cardinality);
                default -> null;
            };
            message.put(parts[0], value);
            List<Integer> attrsToPopulate = new ArrayList<>();
            List<Map<String, Object>> attributes = new ArrayList<Map<String, Object>>();

            if (!full) {
                Set<Integer> attrSubsets = new HashSet();
                //Pick a non duplicate subset - this may be inefficient - TODO
                while (attrSubsets.size() < NUM_ATTRIBUTES_PER_MESSAGE) {
                    int attrSubset = rng.nextInt(NUM_TOTAL_ATTRIBUTES);
                    if (!attrSubsets.contains(attrSubset)) {
                        attrSubsets.add(attrSubset);
                    }
                }
            } else {
                attrsToPopulate = IntStream.range(0, NUM_TOTAL_ATTRIBUTES).boxed().toList();
            }

            for (int attr : attrsToPopulate) {
                attributes.add(getAttribute(deviceId, now, attr));
            }
            message.put("attributes", attributes);

        }

        return message;
    }
}
