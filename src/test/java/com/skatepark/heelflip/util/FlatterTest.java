package com.skatepark.heelflip.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class FlatterTest {

    @Test
    public void testFlattenOnPlainJson() {
        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.addProperty("c", 10.3);
        source.addProperty("d", "word");

        List<JsonObject> flattenObjs = Flatter.flatten(source);
        Assert.assertEquals(1, flattenObjs.size());
        Assert.assertEquals(source, flattenObjs.get(0));
    }

    @Test
    public void testFlatten1LevelObj() {
        JsonObject e = new JsonObject();
        e.addProperty("a", 10);
        e.addProperty("b", true);

        JsonObject source = new JsonObject();
        source.addProperty("a", 10);
        source.addProperty("b", true);
        source.addProperty("c", 10.3);
        source.addProperty("d", "word");
        source.add("e", e);

        List<JsonObject> flattenObjs = Flatter.flatten(source);
        Assert.assertEquals(1, flattenObjs.size());

        JsonObject expected = new JsonObject();
        expected.addProperty("a", 10);
        expected.addProperty("b", true);
        expected.addProperty("c", 10.3);
        expected.addProperty("d", "word");
        expected.addProperty("e.a", 10);
        expected.addProperty("e.b", true);

        Assert.assertEquals(expected, flattenObjs.get(0));
    }

    @Test
    public void testFlatten2LevelObj() {
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

        List<JsonObject> flattenObjs = Flatter.flatten(source);
        Assert.assertEquals(1, flattenObjs.size());

        JsonObject expected = new JsonObject();
        expected.addProperty("a", 10);
        expected.addProperty("b", true);
        expected.addProperty("c", 10.3);
        expected.addProperty("d", "word");
        expected.addProperty("e.a", 10);
        expected.addProperty("e.b", true);
        expected.addProperty("e.c.a", 100);
        expected.addProperty("e.c.b", "other");

        Assert.assertEquals(expected, flattenObjs.get(0));
    }

    @Test
    public void testFlatten1Array() {
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

        List<JsonObject> flattenObjs = Flatter.flatten(source);
        Assert.assertEquals(2, flattenObjs.size());

        JsonObject expected0 = new JsonObject();
        expected0.addProperty("a", 10);
        expected0.addProperty("b", true);
        expected0.addProperty("c.a", 10);
        expected0.addProperty("c.b", true);

        JsonObject expected1 = new JsonObject();
        expected1.addProperty("a", 10);
        expected1.addProperty("b", true);
        expected1.addProperty("c.a", 20);
        expected1.addProperty("c.b", false);

        Assert.assertEquals(expected0, flattenObjs.get(0));
        Assert.assertEquals(expected1, flattenObjs.get(1));
    }

    @Test
    public void testFlatten1ArrayIntoObject() {
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

        List<JsonObject> flattenObjs = Flatter.flatten(source);
        Assert.assertEquals(2, flattenObjs.size());

        JsonObject expected0 = new JsonObject();
        expected0.addProperty("a", 10);
        expected0.addProperty("b", true);
        expected0.addProperty("c.a", 15);
        expected0.addProperty("c.b.a", 10);
        expected0.addProperty("c.b.b", true);

        JsonObject expected1 = new JsonObject();
        expected1.addProperty("a", 10);
        expected1.addProperty("b", true);
        expected1.addProperty("c.a", 15);
        expected1.addProperty("c.b.a", 20);
        expected1.addProperty("c.b.b", false);

        Assert.assertEquals(expected0, flattenObjs.get(0));
        Assert.assertEquals(expected1, flattenObjs.get(1));
    }

    @Test
    public void testFlatten1ObjectIntoArray() {
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

        List<JsonObject> flattenObjs = Flatter.flatten(source);
        Assert.assertEquals(2, flattenObjs.size());

        JsonObject expected0 = new JsonObject();
        expected0.addProperty("a", 10);
        expected0.addProperty("b", true);
        expected0.addProperty("c.a", 10);
        expected0.addProperty("c.b", true);
        expected0.addProperty("c.c.x", 15);
        expected0.addProperty("c.c.y", 35);

        JsonObject expected1 = new JsonObject();
        expected1.addProperty("a", 10);
        expected1.addProperty("b", true);
        expected1.addProperty("c.a", 20);
        expected1.addProperty("c.b", false);

        Assert.assertEquals(expected0, flattenObjs.get(0));
        Assert.assertEquals(expected1, flattenObjs.get(1));
    }
}
