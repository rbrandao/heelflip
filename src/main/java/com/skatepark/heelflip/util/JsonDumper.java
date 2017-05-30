package com.skatepark.heelflip.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;

import com.skatepark.heelflip.ColumnAgg;
import com.skatepark.heelflip.GroupByAgg;
import com.skatepark.heelflip.JsonAgg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class JsonDumper {

    public static void dumpGroupByAgg(JsonAgg jsonAgg, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            Set<String> columnNames = jsonAgg.columnNames();

            JsonArray groupByArray = new JsonArray();
            for (String columnName : columnNames) {
                for (String groupByColumn : columnNames) {
                    if (groupByColumn.equals(columnName)) {
                        continue;
                    }
                    GroupByAgg groupByAgg = jsonAgg.getGroupBy(columnName, groupByColumn);
                    if (groupByAgg == null) {
                        continue;
                    }
                    groupByArray.add(groupByAgg.toJSON());
                }
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            writer.write(gson.toJson(groupByArray));
        }
    }

    public static void dumpColumnAgg(JsonAgg jsonAgg, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            JsonArray columnAggArray = new JsonArray();
            for (String name : jsonAgg.columnNames()) {
                ColumnAgg columnAgg = jsonAgg.getColumnAgg(name);
                columnAggArray.add(columnAgg.toJSON());
            }

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            writer.write(gson.toJson(columnAggArray));
        }
    }
}
