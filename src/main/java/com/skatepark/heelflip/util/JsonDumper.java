package com.skatepark.heelflip.util;

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
            FieldAgg fieldAgg = jsonAgg.getFieldAgg(fieldName);

            String fileName = String.format("__%s.json", fieldName);
            Path filePath = Paths.get(dirPath.toString(), fileName);

            Files.write(filePath, fieldAgg.toString(includeValues).getBytes());
        }

        // group by aggregations
        Set<String> fieldNames = jsonAgg.fieldNames();
        for (String fieldName : fieldNames) {
            for (String groupBy : fieldNames) {
                if (groupBy.equals(fieldName)) {
                    continue;
                }
                GroupByAgg groupByAgg = jsonAgg.getGroupBy(fieldName, groupBy);
                if (groupByAgg == null) {
                    continue;
                }

                String fileName = String.format("%s_groupBy_%s.json", fieldName, groupBy);
                Path filePath = Paths.get(dirPath.toString(), fileName);

                Files.write(filePath, groupByAgg.toString(includeValues).getBytes());
            }
        }
    }
}
