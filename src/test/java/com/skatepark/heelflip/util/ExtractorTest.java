package com.skatepark.heelflip.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ExtractorTest {

    @Test
    public void testExtractOnPlainJson() {
        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.addProperty("c", 10.3);
        source.addProperty("d", "word");

        Map<String, List<JsonPrimitive>> values = Extractor.extract(source);

        Assert.assertEquals(4, values.size());
        Assert.assertEquals(1, values.get("a").size());
        Assert.assertEquals(1, values.get("b").size());
        Assert.assertEquals(1, values.get("c").size());
        Assert.assertEquals(1, values.get("d").size());

        Assert.assertEquals(new JsonPrimitive(10), values.get("a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("b").get(0));
        Assert.assertEquals(new JsonPrimitive(10.3), values.get("c").get(0));
        Assert.assertEquals(new JsonPrimitive("word"), values.get("d").get(0));
    }

    @Test
    public void testExtract1LevelObj() {
        JsonObject e = new JsonObject();
        e.addProperty("a", 10);
        e.addProperty("b", true);

        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.addProperty("c", 10.3);
        source.addProperty("d", "word");
        source.add("e", e);

        Map<String, List<JsonPrimitive>> values = Extractor.extract(source);

        Assert.assertEquals(6, values.size());
        Assert.assertEquals(1, values.get("a").size());
        Assert.assertEquals(1, values.get("b").size());
        Assert.assertEquals(1, values.get("c").size());
        Assert.assertEquals(1, values.get("d").size());
        Assert.assertEquals(1, values.get("e.a").size());
        Assert.assertEquals(1, values.get("e.b").size());

        Assert.assertEquals(new JsonPrimitive(10), values.get("a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("b").get(0));
        Assert.assertEquals(new JsonPrimitive(10.3), values.get("c").get(0));
        Assert.assertEquals(new JsonPrimitive("word"), values.get("d").get(0));
        Assert.assertEquals(new JsonPrimitive(10), values.get("e.a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("e.b").get(0));
    }

    @Test
    public void testExtract2LevelObj() {
        JsonObject c = new JsonObject();
        c.addProperty("a", 100);
        c.addProperty("b", "other");

        JsonObject e = new JsonObject();
        e.addProperty("a", 10);
        e.addProperty("b", true);
        e.add("c", c);

        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.addProperty("c", 10.3);
        source.addProperty("d", "word");
        source.add("e", e);

        Map<String, List<JsonPrimitive>> values = Extractor.extract(source);

        Assert.assertEquals(8, values.size());
        Assert.assertEquals(1, values.get("a").size());
        Assert.assertEquals(1, values.get("b").size());
        Assert.assertEquals(1, values.get("c").size());
        Assert.assertEquals(1, values.get("d").size());
        Assert.assertEquals(1, values.get("e.a").size());
        Assert.assertEquals(1, values.get("e.b").size());
        Assert.assertEquals(1, values.get("e.c.a").size());
        Assert.assertEquals(1, values.get("e.c.b").size());

        Assert.assertEquals(new JsonPrimitive(10), values.get("a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("b").get(0));
        Assert.assertEquals(new JsonPrimitive(10.3), values.get("c").get(0));
        Assert.assertEquals(new JsonPrimitive("word"), values.get("d").get(0));
        Assert.assertEquals(new JsonPrimitive(10), values.get("e.a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("e.b").get(0));
        Assert.assertEquals(new JsonPrimitive(100), values.get("e.c.a").get(0));
        Assert.assertEquals(new JsonPrimitive("other"), values.get("e.c.b").get(0));
    }

    @Test
    public void testExtract1Array() {
        JsonObject elem0 = new JsonObject();
        elem0.addProperty("a", 10);
        elem0.addProperty("b", true);

        JsonObject elem1 = new JsonObject();
        elem1.addProperty("a", 20);
        elem1.addProperty("b", false);

        JsonArray array = new JsonArray();
        array.add(elem0);
        array.add(elem1);

        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.add("c", array);

        Map<String, List<JsonPrimitive>> values = Extractor.extract(source);

        Assert.assertEquals(4, values.size());
        Assert.assertEquals(1, values.get("a").size());
        Assert.assertEquals(1, values.get("b").size());
        Assert.assertEquals(2, values.get("c.a").size());
        Assert.assertEquals(2, values.get("c.b").size());

        Assert.assertEquals(new JsonPrimitive(10), values.get("a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("b").get(0));
        Assert.assertEquals(new JsonPrimitive(10), values.get("c.a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("c.b").get(0));
        Assert.assertEquals(new JsonPrimitive(20), values.get("c.a").get(1));
        Assert.assertEquals(new JsonPrimitive(false), values.get("c.b").get(1));
    }

    @Test
    public void testExtract1ArrayIntoObject() {
        JsonObject elem0 = new JsonObject();
        elem0.addProperty("a", 10);
        elem0.addProperty("b", true);

        JsonObject elem1 = new JsonObject();
        elem1.addProperty("a", 20);
        elem1.addProperty("b", false);

        JsonArray array = new JsonArray();
        array.add(elem0);
        array.add(elem1);

        JsonObject c = new JsonObject();
        c.addProperty("a", 15);
        c.add("b", array);

        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.add("c", c);

        Map<String, List<JsonPrimitive>> values = Extractor.extract(source);

        Assert.assertEquals(5, values.size());
        Assert.assertEquals(1, values.get("a").size());
        Assert.assertEquals(1, values.get("b").size());
        Assert.assertEquals(1, values.get("c.a").size());
        Assert.assertEquals(2, values.get("c.b.a").size());
        Assert.assertEquals(2, values.get("c.b.b").size());

        Assert.assertEquals(new JsonPrimitive(10), values.get("a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("b").get(0));
        Assert.assertEquals(new JsonPrimitive(15), values.get("c.a").get(0));
        Assert.assertEquals(new JsonPrimitive(10), values.get("c.b.a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("c.b.b").get(0));
        Assert.assertEquals(new JsonPrimitive(20), values.get("c.b.a").get(1));
        Assert.assertEquals(new JsonPrimitive(false), values.get("c.b.b").get(1));
    }

    @Test
    public void testExtract1ObjectIntoArray() {
        JsonObject c = new JsonObject();
        c.addProperty("x", 15);
        c.addProperty("y", 35);

        JsonObject elem0 = new JsonObject();
        elem0.addProperty("a", 10);
        elem0.addProperty("b", true);
        elem0.add("c", c);

        JsonObject elem1 = new JsonObject();
        elem1.addProperty("a", 20);
        elem1.addProperty("b", false);

        JsonArray array = new JsonArray();
        array.add(elem0);
        array.add(elem1);

        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.add("c", array);

        Map<String, List<JsonPrimitive>> values = Extractor.extract(source);

        Assert.assertEquals(6, values.size());
        Assert.assertEquals(1, values.get("a").size());
        Assert.assertEquals(1, values.get("b").size());
        Assert.assertEquals(2, values.get("c.a").size());
        Assert.assertEquals(2, values.get("c.b").size());
        Assert.assertEquals(1, values.get("c.c.x").size());
        Assert.assertEquals(1, values.get("c.c.y").size());

        Assert.assertEquals(new JsonPrimitive(10), values.get("a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("b").get(0));
        Assert.assertEquals(new JsonPrimitive(10), values.get("c.a").get(0));
        Assert.assertEquals(new JsonPrimitive(true), values.get("c.b").get(0));
        Assert.assertEquals(new JsonPrimitive(15), values.get("c.c.x").get(0));
        Assert.assertEquals(new JsonPrimitive(35), values.get("c.c.y").get(0));
        Assert.assertEquals(new JsonPrimitive(20), values.get("c.a").get(1));
        Assert.assertEquals(new JsonPrimitive(false), values.get("c.b").get(1));
    }
}
