package com.skatepark.heelflip.table;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.skatepark.heelflip.util.JSONFlatter;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class HeelflipTest {

    //http://jsonstudio.com/resources/
    private static final String STOCKS_FILE_PATH = "stocks.json";

    @Test
    public void foo() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(STOCKS_FILE_PATH);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            JsonParser parser = new JsonParser();

            List<JsonElement> jsons = reader.lines()
                    .map(line -> parser.parse(line))
                    .collect(Collectors.toList());

            for (JsonElement elem : jsons) {
                List<JsonObject> planner = JSONFlatter.flatten(elem.getAsJsonObject());
                planner.forEach(System.out::println);
            }
        }
    }
}
