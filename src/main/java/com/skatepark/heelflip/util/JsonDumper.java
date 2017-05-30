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

public class JsonDumper {

    public static void dumpGroupByAgg(JsonAgg jsonAgg, String pathStr, boolean includeValues) throws IOException {
        Path path = Paths.get(pathStr);
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

    public static void dumpFieldAgg(JsonAgg jsonAgg, String pathStr, boolean includeValues) throws IOException {
        Path path = Paths.get(pathStr);
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
}
