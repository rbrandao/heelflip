package com.skatepark.heelflip.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

public class ExtractorTest {

    @Test
    public void testExtractOnPlainJson() {
        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.addProperty("c", 10.3);
        source.addProperty("d", "word");

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, source);
        Assert.assertEquals(4, values.size());

        Assert.assertEquals(new HFValue("a", id, source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue("b", id, source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue("c", id, source.getAsJsonPrimitive("c")), values.get(2));
        Assert.assertEquals(new HFValue("d", id, source.getAsJsonPrimitive("d")), values.get(3));
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

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, source);

        Assert.assertEquals(6, values.size());

        Assert.assertEquals(new HFValue("a", id, source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue("b", id, source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue("c", id, source.getAsJsonPrimitive("c")), values.get(2));
        Assert.assertEquals(new HFValue("d", id, source.getAsJsonPrimitive("d")), values.get(3));
        Assert.assertEquals(new HFValue("e.a", id, e.getAsJsonPrimitive("a")), values.get(4));
        Assert.assertEquals(new HFValue("e.b", id, e.getAsJsonPrimitive("b")), values.get(5));
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

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, source);
        Assert.assertEquals(8, values.size());

        Assert.assertEquals(new HFValue("a", id, source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue("b", id, source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue("c", id, source.getAsJsonPrimitive("c")), values.get(2));
        Assert.assertEquals(new HFValue("d", id, source.getAsJsonPrimitive("d")), values.get(3));
        Assert.assertEquals(new HFValue("e.a", id, e.getAsJsonPrimitive("a")), values.get(4));
        Assert.assertEquals(new HFValue("e.b", id, e.getAsJsonPrimitive("b")), values.get(5));
        Assert.assertEquals(new HFValue("e.c.a", id, c.getAsJsonPrimitive("a")), values.get(6));
        Assert.assertEquals(new HFValue("e.c.b", id, c.getAsJsonPrimitive("b")), values.get(7));
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

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, source);
        Assert.assertEquals(6, values.size());

        Assert.assertEquals(new HFValue("a", id, source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue("b", id, source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue("c.a", id, elem0.getAsJsonPrimitive("a")), values.get(2));
        Assert.assertEquals(new HFValue("c.b", id, elem0.getAsJsonPrimitive("b")), values.get(3));
        Assert.assertEquals(new HFValue("c.a", id, elem1.getAsJsonPrimitive("a")), values.get(4));
        Assert.assertEquals(new HFValue("c.b", id, elem1.getAsJsonPrimitive("b")), values.get(5));
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

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, source);
        Assert.assertEquals(7, values.size());

        Assert.assertEquals(new HFValue("a", id, source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue("b", id, source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue("c.a", id, c.getAsJsonPrimitive("a")), values.get(2));
        Assert.assertEquals(new HFValue("c.b.a", id, elem0.getAsJsonPrimitive("a")), values.get(3));
        Assert.assertEquals(new HFValue("c.b.b", id, elem0.getAsJsonPrimitive("b")), values.get(4));
        Assert.assertEquals(new HFValue("c.b.a", id, elem1.getAsJsonPrimitive("a")), values.get(5));
        Assert.assertEquals(new HFValue("c.b.b", id, elem1.getAsJsonPrimitive("b")), values.get(6));
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

        UUID id = UUID.randomUUID();
        List<HFValue> values = Extractor.extract(id, source);
        Assert.assertEquals(8, values.size());

        Assert.assertEquals(new HFValue("a", id, source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue("b", id, source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue("c.a", id, elem0.getAsJsonPrimitive("a")), values.get(2));
        Assert.assertEquals(new HFValue("c.b", id, elem0.getAsJsonPrimitive("b")), values.get(3));
        Assert.assertEquals(new HFValue("c.c.x", id, c.getAsJsonPrimitive("x")), values.get(4));
        Assert.assertEquals(new HFValue("c.c.y", id, c.getAsJsonPrimitive("y")), values.get(5));
        Assert.assertEquals(new HFValue("c.a", id, elem1.getAsJsonPrimitive("a")), values.get(6));
        Assert.assertEquals(new HFValue("c.b", id, elem1.getAsJsonPrimitive("b")), values.get(7));
    }
}
