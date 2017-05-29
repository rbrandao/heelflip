package com.skatepark.heelflip.table;

import com.google.gson.JsonPrimitive;

import com.skatepark.heelflip.table.agg.GroupByAgg;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class HeelflipTest {

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

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Set<String> names = heelflip.columnNames();
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

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

        Assert.assertEquals(0, heelflip.getColumnAgg("a").getMin().intValue());
        Assert.assertEquals(0, heelflip.getColumnAgg("a").getMax().intValue());
        Assert.assertEquals(0, heelflip.getColumnAgg("a").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("a").count("0"));

        Assert.assertEquals(1, heelflip.getColumnAgg("b").getMin().intValue());
        Assert.assertEquals(1, heelflip.getColumnAgg("b").getMax().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("b").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("b").count("1"));

        Assert.assertEquals(2, heelflip.getColumnAgg("c").getMin().intValue());
        Assert.assertEquals(2, heelflip.getColumnAgg("c").getMax().intValue());
        Assert.assertEquals(40, heelflip.getColumnAgg("c").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("c").count("2"));

        Assert.assertEquals(3, heelflip.getColumnAgg("d").getMin().intValue());
        Assert.assertEquals(3, heelflip.getColumnAgg("d").getMax().intValue());
        Assert.assertEquals(60, heelflip.getColumnAgg("d").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("d").count("3"));

        Assert.assertEquals(4, heelflip.getColumnAgg("e").getMin().intValue());
        Assert.assertEquals(4, heelflip.getColumnAgg("e").getMax().intValue());
        Assert.assertEquals(80, heelflip.getColumnAgg("e").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("e").count("4"));

        Assert.assertEquals(5, heelflip.getColumnAgg("f").getMin().intValue());
        Assert.assertEquals(5, heelflip.getColumnAgg("f").getMax().intValue());
        Assert.assertEquals(100, heelflip.getColumnAgg("f").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("f").count("5"));

        Assert.assertEquals(6, heelflip.getColumnAgg("g").getMin().intValue());
        Assert.assertEquals(6, heelflip.getColumnAgg("g").getMax().intValue());
        Assert.assertEquals(120, heelflip.getColumnAgg("g").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("g").count("6"));

        Assert.assertEquals(7, heelflip.getColumnAgg("h").getMin().intValue());
        Assert.assertEquals(7, heelflip.getColumnAgg("h").getMax().intValue());
        Assert.assertEquals(140, heelflip.getColumnAgg("h").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("h").count("7"));

        Assert.assertEquals(8, heelflip.getColumnAgg("i").getMin().intValue());
        Assert.assertEquals(8, heelflip.getColumnAgg("i").getMax().intValue());
        Assert.assertEquals(160, heelflip.getColumnAgg("i").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("i").count("8"));

        Assert.assertEquals(9, heelflip.getColumnAgg("j").getMin().intValue());
        Assert.assertEquals(9, heelflip.getColumnAgg("j").getMax().intValue());
        Assert.assertEquals(180, heelflip.getColumnAgg("j").getSum().intValue());
        Assert.assertEquals(20, heelflip.getColumnAgg("j").count("9"));

    }

    @Test
    public void testGetGroupByOnPlainJson() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_00);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(10, heelflip.size());

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

                GroupByAgg groupByAgg = heelflip.getGroupBy(columnName, groupBy);
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

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(3, heelflip.size());

        Set<String> names = heelflip.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b.x"));
        Assert.assertTrue(names.contains("b.y"));
        Assert.assertFalse(names.contains("c"));
    }

    @Test
    public void testGetColumnAggOnJsonArrayWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_01);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(3, heelflip.size());

        Assert.assertNull(heelflip.getColumnAgg("a").getMin());
        Assert.assertNull(heelflip.getColumnAgg("a").getMax());
        Assert.assertNull(heelflip.getColumnAgg("a").getSum());

        Assert.assertEquals(9, heelflip.getColumnAgg("b.x").getMin().intValue());
        Assert.assertEquals(21, heelflip.getColumnAgg("b.x").getMax().intValue());
        Assert.assertEquals(97, heelflip.getColumnAgg("b.x").getSum().intValue());

        Assert.assertEquals(-20, heelflip.getColumnAgg("b.y").getMin().intValue());
        Assert.assertEquals(-1, heelflip.getColumnAgg("b.y").getMax().intValue());
        Assert.assertEquals(-83, heelflip.getColumnAgg("b.y").getSum().intValue());
    }

    @Test
    public void testGetGroupByOnJsonArrayWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_01);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(3, heelflip.size());

        GroupByAgg groupByAgg = heelflip.getGroupBy("b.x", "a");
        Set<String> values = groupByAgg.groupBy("true");
        Assert.assertEquals(3, values.size());

        Assert.assertTrue(values.contains("9"));
        Assert.assertTrue(values.contains("10"));
        Assert.assertTrue(values.contains("12"));

        //TODO
    }

    @Test
    public void testColumnNamesOnJsonArrayWithPrimitive() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_02);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(5, heelflip.size());

        Set<String> names = heelflip.columnNames();
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

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(5, heelflip.size());

        Assert.assertNull(heelflip.getColumnAgg("a").getMin());
        Assert.assertNull(heelflip.getColumnAgg("a").getMax());
        Assert.assertNull(heelflip.getColumnAgg("a").getSum());

        Assert.assertEquals(2, heelflip.getColumnAgg("b_0").getMin().intValue());
        Assert.assertEquals(9, heelflip.getColumnAgg("b_0").getMax().intValue());
        Assert.assertEquals(24, heelflip.getColumnAgg("b_0").getSum().intValue());

        Assert.assertEquals(-8, heelflip.getColumnAgg("b_1").getMin().intValue());
        Assert.assertEquals(-1, heelflip.getColumnAgg("b_1").getMax().intValue());
        Assert.assertEquals(-15, heelflip.getColumnAgg("b_1").getSum().intValue());

        Assert.assertEquals(10, heelflip.getColumnAgg("b_2").getMin().intValue());
        Assert.assertEquals(10, heelflip.getColumnAgg("b_2").getMax().intValue());
        Assert.assertEquals(40, heelflip.getColumnAgg("b_2").getSum().intValue());

        Assert.assertEquals(0, heelflip.getColumnAgg("b_3").getMin().intValue());
        Assert.assertEquals(0, heelflip.getColumnAgg("b_3").getMax().intValue());
        Assert.assertEquals(0, heelflip.getColumnAgg("b_3").getSum().intValue());
    }

    @Test
    public void testColumnNamesOnJsonWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_03);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(3, heelflip.size());

        Set<String> names = heelflip.columnNames();
        Assert.assertTrue(names.contains("a"));
        Assert.assertTrue(names.contains("b.x"));
        Assert.assertTrue(names.contains("b.y"));
        Assert.assertFalse(names.contains("c"));
    }

    @Test
    public void testGetColumnAggOnJsonWithObject() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(SAMPLE_03);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(3, heelflip.size());

        Assert.assertNull(heelflip.getColumnAgg("a").getMin());
        Assert.assertNull(heelflip.getColumnAgg("a").getMax());
        Assert.assertNull(heelflip.getColumnAgg("a").getSum());

        Assert.assertEquals(-1, heelflip.getColumnAgg("b.x").getMin().intValue());
        Assert.assertEquals(1, heelflip.getColumnAgg("b.x").getMax().intValue());
        Assert.assertEquals(0, heelflip.getColumnAgg("b.x").getSum().intValue());

        Assert.assertEquals(2, heelflip.getColumnAgg("b.y").getMin().intValue());
        Assert.assertEquals(6, heelflip.getColumnAgg("b.y").getMax().intValue());
        Assert.assertEquals(12, heelflip.getColumnAgg("b.y").getSum().intValue());
    }

    //    @Test
    public void testLoadLargeFiles() throws IOException {
        InputStream stream = getClass().getClassLoader().getResourceAsStream(STOCKS_FILE_PATH);

        Heelflip heelflip = new Heelflip();
        heelflip.loadNDJSON(stream);
        Assert.assertEquals(69, heelflip.size());

        stream = getClass().getClassLoader().getResourceAsStream(ZIPS_FILE_PATH);
        heelflip.loadNDJSON(stream);

        Assert.assertEquals(75, heelflip.size());
    }
}
