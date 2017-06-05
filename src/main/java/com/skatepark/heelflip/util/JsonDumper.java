package com.skatepark.heelflip.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.skatepark.heelflip.FieldAgg;
import com.skatepark.heelflip.GroupByAgg;
import com.skatepark.heelflip.JsonAgg;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

/**
 * An utility to write a JSON file with all aggregations from the given {@link JsonAgg}.
 *
 * @author greatjapa
 */
public class JsonDumper {

    /**
     * Dump all {@link GroupByAgg} objects into a single file.
     *
     * @param jsonAgg       source object.
     * @param dirPathStr    path for directory destination.
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @throws IOException if IO errors occurs.
     */
    public static void dumpReport(JsonAgg jsonAgg, String dirPathStr, boolean includeValues) throws IOException {
        Path dirPath = Paths.get(dirPathStr);

        if (!Files.exists(dirPath)) {
            Files.createDirectory(dirPath);
        }

        // global aggregations
        for (String fieldName : jsonAgg.fieldNames()) {
            Path fieldDir = Paths.get(dirPath.toString(), fieldName);
            Files.createDirectory(fieldDir);

            FieldAgg fieldAgg = jsonAgg.getFieldAgg(fieldName);

            String fileName = String.format("__%s.json", fieldName);
            Path filePath = Paths.get(fieldDir.toString(), fileName);

            Files.write(filePath, fieldAgg.toString(includeValues).getBytes());
        }

        // group by aggregations
        Set<String> fieldNames = jsonAgg.fieldNames();
        JsonArray missingGroupByArray = new JsonArray();
        for (String fieldName : fieldNames) {
            Path fieldDir = Paths.get(dirPath.toString(), fieldName);

            for (String groupBy : fieldNames) {
                if (groupBy.equals(fieldName)) {
                    continue;
                }
                GroupByAgg groupByAgg = jsonAgg.getGroupBy(fieldName, groupBy);
                if (groupByAgg == null) {
                    JsonObject missingEntry = new JsonObject();
                    missingEntry.addProperty("field", fieldName);
                    missingEntry.addProperty("groupBy", groupBy);
                    missingGroupByArray.add(missingEntry);
                    continue;
                }

                String fileName = String.format("%s_groupBy_%s.json", fieldName, groupBy);
                Path filePath = Paths.get(fieldDir.toString(), fileName);

                Files.write(filePath, groupByAgg.toString(includeValues).getBytes());
            }
        }

        if (missingGroupByArray.size() > 0) {
            Path path = Paths.get(dirPath.toString(), "__missingGroupBy.");
            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            Files.write(path, gson.toJson(missingGroupByArray).getBytes());
        }
    }
}
