package com.skatepark.heelflip.util;

import com.skatepark.heelflip.table.Heelflip;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class Report {

    public String getAsString(Heelflip heelflip) {
        String ln = System.lineSeparator();

        Set<String> columnNames = heelflip.columnNames();
        StringBuilder builder = new StringBuilder();
        builder.append("-- Number of columns: ").append(columnNames.size()).append(ln);
        builder.append("-- Column names: ").append(ln);
        columnNames.forEach(name -> builder.append(name).append(ln));
        builder.append(ln);

        for (String name : columnNames) {
            long count = heelflip.count(name);
            long minAsLong = heelflip.minAsLong(name);
            long maxAsLong = heelflip.maxAsLong(name);
            double minAsDouble = heelflip.minAsDouble(name);
            long sumAsLong = heelflip.sumAsLong(name);
            double maxAsDouble = heelflip.maxAsDouble(name);
            double sumAsDouble = heelflip.sumAsDouble(name);

            builder.append("-- Column: ").append(name).append(ln);
            builder.append("-- Count: ").append(count).append(ln);
            builder.append("-- Min as Long: ").append(minAsLong).append(ln);
            builder.append("-- Max as Long: ").append(maxAsLong).append(ln);
            builder.append("-- Sum as Long: ").append(sumAsLong).append(ln);
            builder.append("-- Min as Double: ").append(minAsDouble).append(ln);
            builder.append("-- Max as Double: ").append(maxAsDouble).append(ln);
            builder.append("-- Sum as Double: ").append(sumAsDouble).append(ln);
            builder.append(ln);
        }
        return builder.toString();
    }

    public void write(Heelflip heelflip, String pathStr) throws IOException {
        Path path = Paths.get(pathStr);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writer.write(getAsString(heelflip));
        }
    }
}
