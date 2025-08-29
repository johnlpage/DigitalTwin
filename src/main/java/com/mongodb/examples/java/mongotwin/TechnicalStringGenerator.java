package com.mongodb.examples.java.mongotwin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class TechnicalStringGenerator {
    private final FastRCG rng = new FastRCG();
    private final String[] technicalTerms;
    int nTerms;
    StringBuilder sb = new StringBuilder();

    public TechnicalStringGenerator() {
        this.technicalTerms = loadTermsFromResource("/technical-terms.txt").toArray(new String[0]);
        nTerms = technicalTerms.length;

    }

    private List<String> loadTermsFromResource(String resourcePath) {
        List<String> terms = new ArrayList<>();
        try (InputStream is = getClass().getResourceAsStream(resourcePath);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    terms.add(line);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load technical terms from resource", e);
        }
        return terms;
    }

    String generateString(int seed, int targetLength) {
        sb.setLength(0);
        rng.setSeed(seed);

        while (sb.length() < targetLength) {
            int choice = rng.nextInt(nTerms);
            sb.append(technicalTerms[choice]);
            sb.append(" ");
        }

        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
}
