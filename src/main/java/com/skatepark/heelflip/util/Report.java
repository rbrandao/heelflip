package com.skatepark.heelflip.util;

import com.skatepark.heelflip.table.ColumnStatistic;
import com.skatepark.heelflip.table.Heelflip;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigDecimal;
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

            ColumnStatistic statistics = heelflip.getStatistics(name);
            int stringCount = statistics.getStringCount();
            int booleanCount = statistics.getBooleanCount();
            int numberCount = statistics.getNumberCount();

            builder.append("-- Column: ").append(name).append(ln);
            builder.append("-- Count: ").append(count).append(ln);
            builder.append("-- String count:  ").append(stringCount).append(ln);
            builder.append("-- Boolean count: ").append(booleanCount).append(ln);
            builder.append("-- Number count:  ").append(numberCount).append(ln);

            BigDecimal min = statistics.getMin();
            BigDecimal max = statistics.getMax();
            BigDecimal sum = statistics.getSum();
            if (min != null && max != null & sum != null) {
                builder.append("-- Min: ").append(min.longValue()).append(ln);
                builder.append("-- Max: ").append(max.longValue()).append(ln);
                builder.append("-- Sum: ").append(sum.longValue()).append(ln);
            }
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
