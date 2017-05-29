package com.skatepark.heelflip.table;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import com.skatepark.heelflip.table.agg.ColumnAgg;
import com.skatepark.heelflip.table.agg.GroupByAgg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

class JSONDumper {

    static void dump(Heelflip heelflip, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            Set<String> columnNames = heelflip.columnNames();

            JsonArray columnAggArray = new JsonArray();
            for (String name : columnNames) {
                ColumnAgg columnAgg = heelflip.getColumnAgg(name);
                columnAggArray.add(columnAgg.toJSON());
            }

            JsonArray groupByArray = new JsonArray();
            for (String columnName : columnNames) {
                for (String groupByColumn : columnNames) {
                    if (groupByColumn.equals(columnName)) {
                        continue;
                    }
                    GroupByAgg groupByAgg = heelflip.getGroupBy(columnName, groupByColumn);
                    if (groupByAgg == null) {
                        continue;
                    }
                    groupByArray.add(groupByAgg.toJSON());
                }
            }

            JsonObject result = new JsonObject();
            result.add("columnAggs", columnAggArray);
            result.add("groupByAggs", groupByArray);

            Gson gson = new GsonBuilder().setPrettyPrinting().create();
            writer.write(gson.toJson(result));
        }
    }
}
