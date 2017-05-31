package com.skatepark.heelflip.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;

import com.skatepark.heelflip.FieldAgg;
import com.skatepark.heelflip.GroupByAgg;
import com.skatepark.heelflip.JsonAgg;

import java.io.BufferedWriter;
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
     * Dump all {@link FieldAgg} objects into a single file.
     *
     * @param jsonAgg       source object.
     * @param filePath      path destination.
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @throws IOException if IO errors occurs.
     */
    public static void dumpFieldAgg(JsonAgg jsonAgg, String filePath, boolean includeValues) throws IOException {
        Path path = Paths.get(filePath);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            JsonArray fieldAggArray = new JsonArray();
            for (String name : jsonAgg.fieldNames()) {
                FieldAgg fieldAgg = jsonAgg.getFieldAgg(name);
                fieldAggArray.add(fieldAgg.toJSON(includeValues));
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            writer.write(gson.toJson(fieldAggArray));
        }
    }

    /**
     * Dump all {@link GroupByAgg} objects into a single file.
     *
     * @param jsonAgg       source object.
     * @param filePath      path destination.
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @throws IOException if IO errors occurs.
     */
    public static void dumpGroupByAgg(JsonAgg jsonAgg, String filePath, boolean includeValues) throws IOException {
        Path path = Paths.get(filePath);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            Set<String> fieldNames = jsonAgg.fieldNames();

            JsonArray groupByArray = new JsonArray();
            for (String fieldName : fieldNames) {
                for (String groupBy : fieldNames) {
                    if (groupBy.equals(fieldName)) {
                        continue;
                    }
                    GroupByAgg groupByAgg = jsonAgg.getGroupBy(fieldName, groupBy);
                    if (groupByAgg == null) {
                        continue;
                    }
                    groupByArray.add(groupByAgg.toJSON(includeValues));
                }
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            writer.write(gson.toJson(groupByArray));
        }
    }
}
