package com.skatepark.heelflip.util;

import com.skatepark.heelflip.table.ColumnStatistic;
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

            ColumnStatistic statistics = heelflip.getStatistics(name);
            int booleanCount = statistics.getBooleanCount();
            int stringCount = statistics.getStringCount();
            int longCount = statistics.getLongCount();
            int doubleCount = statistics.getDoubleCount();

            builder.append("-- Column: ").append(name).append(ln);
            builder.append("-- Count: ").append(count).append(ln);
            builder.append("-- Boolean count: ").append(booleanCount).append(ln);
            builder.append("-- String count:  ").append(stringCount).append(ln);
            builder.append("-- Long count:    ").append(longCount).append(ln);
            builder.append("-- Double count:  ").append(doubleCount).append(ln);

            if (doubleCount > Math.max(Math.max(booleanCount, stringCount), longCount)) {
                double min = statistics.getMin().doubleValue();
                double max = statistics.getMax().doubleValue();
                double sum = statistics.getSum().doubleValue();
                builder.append("-- Min as Double: ").append(min).append(ln);
                builder.append("-- Max as Double: ").append(max).append(ln);
                builder.append("-- Sum as Double: ").append(sum).append(ln);
            } else {
                long min = statistics.getMin().longValue();
                long max = statistics.getMax().longValue();
                long sum = statistics.getSum().longValue();
                builder.append("-- Min as Long: ").append(min).append(ln);
                builder.append("-- Max as Long: ").append(max).append(ln);
                builder.append("-- Sum as Long: ").append(sum).append(ln);
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
