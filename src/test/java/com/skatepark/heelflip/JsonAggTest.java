package com.skatepark.heelflip;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JsonAggTest {

    private static final String SAMPLE_00 = "sample00.json";
    private static final String SAMPLE_01 = "sample01.json";
    private static final String SAMPLE_02 = "sample02.json";
    private static final String SAMPLE_03 = "sample03.json";

    //http://jsonstudio.com/resources/
    private static final String STOCKS_FILE_PATH = "stocks.json";
    private static final String ZIPS_FILE_PATH = "zips.json";

    @Test
    public void testColumnNamesOnPlainJson() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(10, jsonAgg.size());

        Set<String> names = jsonAgg.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b"));
        Assert.assertTrue(names.contains("c"));
        Assert.assertTrue(names.contains("d"));
        Assert.assertTrue(names.contains("e"));
        Assert.assertTrue(names.contains("f"));
        Assert.assertTrue(names.contains("g"));
        Assert.assertTrue(names.contains("h"));
        Assert.assertTrue(names.contains("i"));
        Assert.assertTrue(names.contains("j"));
        Assert.assertFalse(names.contains("l"));
    }

    @Test
    public void testGetColumnAggOnPlainJson() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(10, jsonAgg.size());

        Assert.assertEquals(0, jsonAgg.getColumnAgg("a").getMin().intValue());
        Assert.assertEquals(0, jsonAgg.getColumnAgg("a").getMax().intValue());
        Assert.assertEquals(0, jsonAgg.getColumnAgg("a").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("a").count("0"));

        Assert.assertEquals(1, jsonAgg.getColumnAgg("b").getMin().intValue());
        Assert.assertEquals(1, jsonAgg.getColumnAgg("b").getMax().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("b").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("b").count("1"));

        Assert.assertEquals(2, jsonAgg.getColumnAgg("c").getMin().intValue());
        Assert.assertEquals(2, jsonAgg.getColumnAgg("c").getMax().intValue());
        Assert.assertEquals(40, jsonAgg.getColumnAgg("c").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("c").count("2"));

        Assert.assertEquals(3, jsonAgg.getColumnAgg("d").getMin().intValue());
        Assert.assertEquals(3, jsonAgg.getColumnAgg("d").getMax().intValue());
        Assert.assertEquals(60, jsonAgg.getColumnAgg("d").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("d").count("3"));

        Assert.assertEquals(4, jsonAgg.getColumnAgg("e").getMin().intValue());
        Assert.assertEquals(4, jsonAgg.getColumnAgg("e").getMax().intValue());
        Assert.assertEquals(80, jsonAgg.getColumnAgg("e").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("e").count("4"));

        Assert.assertEquals(5, jsonAgg.getColumnAgg("f").getMin().intValue());
        Assert.assertEquals(5, jsonAgg.getColumnAgg("f").getMax().intValue());
        Assert.assertEquals(100, jsonAgg.getColumnAgg("f").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("f").count("5"));

        Assert.assertEquals(6, jsonAgg.getColumnAgg("g").getMin().intValue());
        Assert.assertEquals(6, jsonAgg.getColumnAgg("g").getMax().intValue());
        Assert.assertEquals(120, jsonAgg.getColumnAgg("g").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("g").count("6"));

        Assert.assertEquals(7, jsonAgg.getColumnAgg("h").getMin().intValue());
        Assert.assertEquals(7, jsonAgg.getColumnAgg("h").getMax().intValue());
        Assert.assertEquals(140, jsonAgg.getColumnAgg("h").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("h").count("7"));

        Assert.assertEquals(8, jsonAgg.getColumnAgg("i").getMin().intValue());
        Assert.assertEquals(8, jsonAgg.getColumnAgg("i").getMax().intValue());
        Assert.assertEquals(160, jsonAgg.getColumnAgg("i").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("i").count("8"));

        Assert.assertEquals(9, jsonAgg.getColumnAgg("j").getMin().intValue());
        Assert.assertEquals(9, jsonAgg.getColumnAgg("j").getMax().intValue());
        Assert.assertEquals(180, jsonAgg.getColumnAgg("j").getSum().intValue());
        Assert.assertEquals(20, jsonAgg.getColumnAgg("j").count("9"));

    }

    @Test
    public void testGetGroupByOnPlainJson() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(10, jsonAgg.size());

        Map<String, String> expected = new HashMap<>();
        expected.put("a", "0");
        expected.put("b", "1");
        expected.put("c", "2");
        expected.put("d", "3");
        expected.put("e", "4");
        expected.put("f", "5");
        expected.put("g", "6");
        expected.put("h", "7");
        expected.put("i", "8");
        expected.put("j", "9");

        for (Map.Entry<String, String> columnEntry : expected.entrySet()) {
            for (Map.Entry<String, String> groupByEntry : expected.entrySet()) {
                String columnName = columnEntry.getKey();
                String groupBy = groupByEntry.getKey();
                if (columnName.equals(groupBy)) {
                    continue;
                }

                String value = columnEntry.getValue();
                String groupByValue = groupByEntry.getValue();

                GroupByAgg groupByAgg = jsonAgg.getGroupBy(columnName, groupBy);
                Assert.assertEquals(1, groupByAgg.values().size());
                Assert.assertEquals(1, groupByAgg.groupByValues().size());

                Set<String> groupBySet = groupByAgg.groupBy(groupByValue);
                Assert.assertEquals(1, groupBySet.size());
                Assert.assertEquals(value, groupBySet.iterator().next());
            }
        }
    }

    @Test
    public void testColumnNamesOnJsonArrayWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_01);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(3, jsonAgg.size());

        Set<String> names = jsonAgg.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b.x"));
        Assert.assertTrue(names.contains("b.y"));
        Assert.assertFalse(names.contains("c"));
    }

    @Test
    public void testGetColumnAggOnJsonArrayWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_01);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(3, jsonAgg.size());

        Assert.assertNull(jsonAgg.getColumnAgg("a").getMin());
        Assert.assertNull(jsonAgg.getColumnAgg("a").getMax());
        Assert.assertNull(jsonAgg.getColumnAgg("a").getSum());

        Assert.assertEquals(9, jsonAgg.getColumnAgg("b.x").getMin().intValue());
        Assert.assertEquals(21, jsonAgg.getColumnAgg("b.x").getMax().intValue());
        Assert.assertEquals(97, jsonAgg.getColumnAgg("b.x").getSum().intValue());

        Assert.assertEquals(-20, jsonAgg.getColumnAgg("b.y").getMin().intValue());
        Assert.assertEquals(-1, jsonAgg.getColumnAgg("b.y").getMax().intValue());
        Assert.assertEquals(-83, jsonAgg.getColumnAgg("b.y").getSum().intValue());
    }

    @Test
    public void testGetGroupByOnJsonArrayWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_01);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(3, jsonAgg.size());

        GroupByAgg groupByAgg = jsonAgg.getGroupBy("b.x", "a");
        Set<String> values = groupByAgg.groupBy("true");
        Assert.assertEquals(3, values.size());
        Assert.assertTrue(values.contains("9"));
        Assert.assertTrue(values.contains("10"));
        Assert.assertTrue(values.contains("12"));

        groupByAgg = jsonAgg.getGroupBy("b.y", "a");
        values = groupByAgg.groupBy("false");
        Assert.assertEquals(4, values.size());
        Assert.assertTrue(values.contains("-12"));
        Assert.assertTrue(values.contains("-13"));
        Assert.assertTrue(values.contains("-2"));
        Assert.assertTrue(values.contains("-1"));

        groupByAgg = jsonAgg.getGroupBy("b.y", "b.x");
        values = groupByAgg.groupBy("10");
        Assert.assertEquals(4, values.size());
        Assert.assertTrue(values.contains("-20"));
        Assert.assertTrue(values.contains("-11"));
        Assert.assertTrue(values.contains("-10"));
        Assert.assertTrue(values.contains("-14"));

        values = groupByAgg.groupBy("9");
        Assert.assertEquals(2, values.size());
        Assert.assertTrue(values.contains("-20"));
        Assert.assertTrue(values.contains("-10"));

        groupByAgg = jsonAgg.getGroupBy("a", "b.x");
        values = groupByAgg.groupBy("10");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains("true"));
    }

    @Test
    public void testColumnNamesOnJsonArrayWithPrimitive() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_02);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(5, jsonAgg.size());

        Set<String> names = jsonAgg.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b_0"));
        Assert.assertTrue(names.contains("b_1"));
        Assert.assertTrue(names.contains("b_2"));
        Assert.assertTrue(names.contains("b_3"));
        Assert.assertFalse(names.contains("c"));
    }

    @Test
    public void testGetColumnAggOnJsonArrayWithPrimitive() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_02);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(5, jsonAgg.size());

        Assert.assertNull(jsonAgg.getColumnAgg("a").getMin());
        Assert.assertNull(jsonAgg.getColumnAgg("a").getMax());
        Assert.assertNull(jsonAgg.getColumnAgg("a").getSum());

        Assert.assertEquals(2, jsonAgg.getColumnAgg("b_0").getMin().intValue());
        Assert.assertEquals(9, jsonAgg.getColumnAgg("b_0").getMax().intValue());
        Assert.assertEquals(24, jsonAgg.getColumnAgg("b_0").getSum().intValue());

        Assert.assertEquals(-8, jsonAgg.getColumnAgg("b_1").getMin().intValue());
        Assert.assertEquals(-1, jsonAgg.getColumnAgg("b_1").getMax().intValue());
        Assert.assertEquals(-15, jsonAgg.getColumnAgg("b_1").getSum().intValue());

        Assert.assertEquals(10, jsonAgg.getColumnAgg("b_2").getMin().intValue());
        Assert.assertEquals(10, jsonAgg.getColumnAgg("b_2").getMax().intValue());
        Assert.assertEquals(40, jsonAgg.getColumnAgg("b_2").getSum().intValue());

        Assert.assertEquals(0, jsonAgg.getColumnAgg("b_3").getMin().intValue());
        Assert.assertEquals(0, jsonAgg.getColumnAgg("b_3").getMax().intValue());
        Assert.assertEquals(0, jsonAgg.getColumnAgg("b_3").getSum().intValue());
    }

    @Test
    public void testGetGroupByOnJsonArrayWithPrimitive() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_02);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(5, jsonAgg.size());

        GroupByAgg groupByAgg = jsonAgg.getGroupBy("b_0", "a");
        Set<String> values = groupByAgg.groupBy("true");
        Assert.assertEquals(2, values.size());
        Assert.assertTrue(values.contains("9"));
        Assert.assertTrue(values.contains("8"));


        groupByAgg = jsonAgg.getGroupBy("b_0", "b_2");
        values = groupByAgg.groupBy("10");
        Assert.assertEquals(4, values.size());
        Assert.assertTrue(values.contains("9"));
        Assert.assertTrue(values.contains("9"));
        Assert.assertTrue(values.contains("2"));
        Assert.assertTrue(values.contains("5"));
    }

    @Test
    public void testColumnNamesOnJsonWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_03);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(3, jsonAgg.size());

        Set<String> names = jsonAgg.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b.x"));
        Assert.assertTrue(names.contains("b.y"));
        Assert.assertFalse(names.contains("c"));
    }

    @Test
    public void testGetColumnAggOnJsonWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_03);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(3, jsonAgg.size());

        Assert.assertNull(jsonAgg.getColumnAgg("a").getMin());
        Assert.assertNull(jsonAgg.getColumnAgg("a").getMax());
        Assert.assertNull(jsonAgg.getColumnAgg("a").getSum());

        Assert.assertEquals(-1, jsonAgg.getColumnAgg("b.x").getMin().intValue());
        Assert.assertEquals(1, jsonAgg.getColumnAgg("b.x").getMax().intValue());
        Assert.assertEquals(0, jsonAgg.getColumnAgg("b.x").getSum().intValue());

        Assert.assertEquals(2, jsonAgg.getColumnAgg("b.y").getMin().intValue());
        Assert.assertEquals(6, jsonAgg.getColumnAgg("b.y").getMax().intValue());
        Assert.assertEquals(12, jsonAgg.getColumnAgg("b.y").getSum().intValue());
    }

    @Test
    public void testGetGroupByOnJsonWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_03);

        JsonAgg jsonAgg = new JsonAgg();
        jsonAgg.loadNDJSON(stream);

        Assert.assertEquals(3, jsonAgg.size());

        GroupByAgg groupByAgg = jsonAgg.getGroupBy("b.x", "a");
        Set<String> values = groupByAgg.groupBy("true");
        Assert.assertEquals(3, values.size());
        Assert.assertTrue(values.contains("-1"));
        Assert.assertTrue(values.contains("0"));
        Assert.assertTrue(values.contains("1"));

        groupByAgg = jsonAgg.getGroupBy("b.y", "b.x");
        values = groupByAgg.groupBy("0");
        Assert.assertEquals(1, values.size());
        Assert.assertTrue(values.contains("4"));
    }

    @Test
    public void testLargeFileStocks() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(STOCKS_FILE_PATH);

        JsonAgg jsonAgg = new JsonAgg();

        long time = System.currentTimeMillis();
        jsonAgg.loadNDJSON(stream);
        System.out.println("stocks takes (seconds): " + (System.currentTimeMillis() - time) / 1000);
        Assert.assertEquals(69, jsonAgg.size());
    }

    @Test
    public void testLargeFileZips() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(ZIPS_FILE_PATH);
        JsonAgg jsonAgg = new JsonAgg();
        long time = System.currentTimeMillis();
        jsonAgg.loadNDJSON(stream);
        System.out.println("zips takes (seconds): " + (System.currentTimeMillis() - time) / 1000);

        Assert.assertEquals(6, jsonAgg.size());
    }
}
