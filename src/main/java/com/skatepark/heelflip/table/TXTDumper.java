package com.skatepark.heelflip.table;

import com.skatepark.heelflip.table.agg.ColumnAgg;
import com.skatepark.heelflip.table.agg.GroupByAgg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

class TXTDumper {

    static void dump(Heelflip heelflip, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            Set<String> columnNames = heelflip.columnNames();

            writer.write("-- Number of columns: ");
            writer.write(Integer.toString(columnNames.size()));
            writer.write(System.lineSeparator());

            writer.write("-- Column names: ");
            writer.write(System.lineSeparator());
            for (String columnName : columnNames) {
                writer.write(columnName);
                writer.write(System.lineSeparator());
            }
            writer.write(System.lineSeparator());

            writer.write(" ================= Column Aggregations ================= ");
            writer.write(System.lineSeparator());

            for (String name : columnNames) {
                ColumnAgg columnAgg = heelflip.getColumnAgg(name);
                writer.write(columnAgg.toString());
                writer.write(System.lineSeparator());
            }

            writer.write(" ================= Group By Aggregations ================= ");
            writer.write(System.lineSeparator());

            for (String columnName : columnNames) {
                for (String groupByColumn : columnNames) {
                    if (groupByColumn.equals(columnName)) {
                        continue;
                    }
                    GroupByAgg groupByAgg = heelflip.getGroupBy(columnName, groupByColumn);
                    if (groupByAgg == null) {
                        writer.write("-- GroupBy: ");
                        writer.write(groupByColumn);
                        writer.write("; ColumnName: ");
                        writer.write(columnName);
                        writer.write("; [NOT FOUND]");
                        writer.write(System.lineSeparator());
                        continue;
                    }

                    writer.write(groupByAgg.toString());
                    writer.write(System.lineSeparator());
                }
            }
        }
    }
}
