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

        Assert.assertEquals(new HFValue(id, "a", source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue(id, "b", source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue(id, "c", source.getAsJsonPrimitive("c")), values.get(2));
        Assert.assertEquals(new HFValue(id, "d", source.getAsJsonPrimitive("d")), values.get(3));
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

        Assert.assertEquals(new HFValue(id, "a", source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue(id, "b", source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue(id, "c", source.getAsJsonPrimitive("c")), values.get(2));
        Assert.assertEquals(new HFValue(id, "d", source.getAsJsonPrimitive("d")), values.get(3));
        Assert.assertEquals(new HFValue(id, "e.a", e.getAsJsonPrimitive("a")), values.get(4));
        Assert.assertEquals(new HFValue(id, "e.b", e.getAsJsonPrimitive("b")), values.get(5));
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

        Assert.assertEquals(new HFValue(id, "a", source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue(id, "b", source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue(id, "c", source.getAsJsonPrimitive("c")), values.get(2));
        Assert.assertEquals(new HFValue(id, "d", source.getAsJsonPrimitive("d")), values.get(3));
        Assert.assertEquals(new HFValue(id, "e.a", e.getAsJsonPrimitive("a")), values.get(4));
        Assert.assertEquals(new HFValue(id, "e.b", e.getAsJsonPrimitive("b")), values.get(5));
        Assert.assertEquals(new HFValue(id, "e.c.a", c.getAsJsonPrimitive("a")), values.get(6));
        Assert.assertEquals(new HFValue(id, "e.c.b", c.getAsJsonPrimitive("b")), values.get(7));
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

        Assert.assertEquals(new HFValue(id, "a", source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue(id, "b", source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue(id, "c.a", elem0.getAsJsonPrimitive("a")), values.get(2));
        Assert.assertEquals(new HFValue(id, "c.b", elem0.getAsJsonPrimitive("b")), values.get(3));
        Assert.assertEquals(new HFValue(id, "c.a", elem1.getAsJsonPrimitive("a")), values.get(4));
        Assert.assertEquals(new HFValue(id, "c.b", elem1.getAsJsonPrimitive("b")), values.get(5));
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

        Assert.assertEquals(new HFValue(id, "a", source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue(id, "b", source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue(id, "c.a", c.getAsJsonPrimitive("a")), values.get(2));
        Assert.assertEquals(new HFValue(id, "c.b.a", elem0.getAsJsonPrimitive("a")), values.get(3));
        Assert.assertEquals(new HFValue(id, "c.b.b", elem0.getAsJsonPrimitive("b")), values.get(4));
        Assert.assertEquals(new HFValue(id, "c.b.a", elem1.getAsJsonPrimitive("a")), values.get(5));
        Assert.assertEquals(new HFValue(id, "c.b.b", elem1.getAsJsonPrimitive("b")), values.get(6));
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

        Assert.assertEquals(new HFValue(id, "a", source.getAsJsonPrimitive("a")), values.get(0));
        Assert.assertEquals(new HFValue(id, "b", source.getAsJsonPrimitive("b")), values.get(1));
        Assert.assertEquals(new HFValue(id, "c.a", elem0.getAsJsonPrimitive("a")), values.get(2));
        Assert.assertEquals(new HFValue(id, "c.b", elem0.getAsJsonPrimitive("b")), values.get(3));
        Assert.assertEquals(new HFValue(id, "c.c.x", c.getAsJsonPrimitive("x")), values.get(4));
        Assert.assertEquals(new HFValue(id, "c.c.y", c.getAsJsonPrimitive("y")), values.get(5));
        Assert.assertEquals(new HFValue(id, "c.a", elem1.getAsJsonPrimitive("a")), values.get(6));
        Assert.assertEquals(new HFValue(id, "c.b", elem1.getAsJsonPrimitive("b")), values.get(7));
    }
}
