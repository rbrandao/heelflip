package com.skatepark.heelflip.table;

import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

public class HeelflipTest {

    //http://jsonstudio.com/resources/
    private static final String STOCKS_FILE_PATH = "stocks.json";
    private static final String ZIPS_FILE_PATH = "zips.json";

    @Test
    public void foo() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(STOCKS_FILE_PATH);

        Heelflip heelflip = new Heelflip("table");
        heelflip.load(stream);

        stream = getClass().getClassLoader().getResourceAsStream(ZIPS_FILE_PATH);
        heelflip.load(stream);

        System.out.println(heelflip.columnNames());
    }
}
