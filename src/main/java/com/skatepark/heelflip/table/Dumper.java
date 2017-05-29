package com.skatepark.heelflip.table;

import com.skatepark.heelflip.table.agg.ColumnAgg;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

class Dumper {

    static void dumpAsTxt(Heelflip heelflip, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {

            Set<String> columnNames = heelflip.columnNames();
            writeLn(writer, "-- Number of columns: ", columnNames.size());

            writeLn(writer, "-- Column names: ");
            for (String columnName : columnNames) {
                writeLn(writer, columnName);
            }
            writeLn(writer);

            for (String name : columnNames) {
                ColumnAgg columnAgg = heelflip.getColumnAgg(name);
                int count = columnAgg.count();
                int cardinality = columnAgg.cardinality();
                int stringCount = columnAgg.getStringCount();
                int booleanCount = columnAgg.getBooleanCount();
                int numberCount = columnAgg.getNumberCount();

                writeLn(writer, "-- Column: ", name);
                writeLn(writer, "** Count:       ", count);
                writeLn(writer, "** Cardinality: ", cardinality);
                writeLn(writer, "** String:      ", stringCount);
                writeLn(writer, "** Boolean:     ", booleanCount);
                writeLn(writer, "** Number:      ", numberCount);

                BigDecimal min = columnAgg.getMin();
                BigDecimal max = columnAgg.getMax();
                BigDecimal sum = columnAgg.getSum();
                if (min != null && max != null & sum != null) {
                    writeLn(writer, "-> Min: ", min.longValue());
                    writeLn(writer, "-> Max: ", max.longValue());
                    writeLn(writer, "-> Sum: ", sum.longValue());
                }
                writeLn(writer);
            }
        }
    }

    private static void writeLn(Writer writer, Object... params) {
        try {
            if (params != null) {
                for (Object param : params) {
                    writer.write(param.toString());
                }
            }
            writer.write(System.lineSeparator());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
