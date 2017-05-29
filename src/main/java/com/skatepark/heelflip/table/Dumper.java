package com.skatepark.heelflip.table;

import com.skatepark.heelflip.table.agg.ColumnAgg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

class Dumper {

    static void dumpAsTxt(Heelflip heelflip, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            Set<String> columnNames = heelflip.columnNames();
            writer.write("-- Number of columns: ");
            writer.write(columnNames.size());
            writer.write(System.lineSeparator());

            writer.write("-- Column names: ");
            for (String columnName : columnNames) {
                writer.write(columnName);
                writer.write(System.lineSeparator());
            }
            writer.write(System.lineSeparator());

            for (String name : columnNames) {
                ColumnAgg columnAgg = heelflip.getColumnAgg(name);
                writer.write(columnAgg.toString());
                writer.write(System.lineSeparator());
            }
        }
    }
}
