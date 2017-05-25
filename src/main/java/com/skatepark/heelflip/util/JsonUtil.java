package com.skatepark.heelflip.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonUtil {

    public static List<JsonObject> flatten(JsonObject json) {
        if (isFlat(json)) {
            return Collections.singletonList(json);
        }

        Map<String, JsonObject> objectsMap = new HashMap<>();
        Map<String, JsonArray> arraysMap = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            String key = entry.getKey();
            JsonElement value = entry.getValue();

            if (value.isJsonObject()) {
                objectsMap.put(key, value.getAsJsonObject());
            }
            if (value.isJsonArray()) {
                arraysMap.put(key, value.getAsJsonArray());
            }
        }

        List<JsonObject> result = new ArrayList<>();
        for (Map.Entry<String, JsonObject> entry : objectsMap.entrySet()) {
            String fieldName = entry.getKey();
            JsonObject obj = entry.getValue();

            JsonObject newJson = new JsonObject();
            shallowCopy(json, newJson, "");

            for (JsonObject flattenObj : flatten(obj)) {
                shallowCopy(flattenObj, newJson, fieldName + ".");
            }
            result.add(newJson);
        }

        for (Map.Entry<String, JsonArray> entry : arraysMap.entrySet()) {
            String fieldName = entry.getKey();
            JsonArray array = entry.getValue();

            for (JsonElement elem : array) {
                if (!elem.isJsonObject()) {
                    continue;
                }
                JsonObject newJson = new JsonObject();
                shallowCopy(json, newJson, "");

                for (JsonObject flattenObj : flatten(elem.getAsJsonObject())) {
                    shallowCopy(flattenObj, newJson, fieldName + ".");
                }
                result.add(newJson);
            }
        }
        return result;
    }

    private static boolean isFlat(JsonObject json) {
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            JsonElement value = entry.getValue();

            if (value.isJsonArray() || value.isJsonObject()) {
                return false;
            }
        }
        return true;
    }

    private static void shallowCopy(JsonObject source, JsonObject target, String prefix) {
        for (Map.Entry<String, JsonElement> entry : source.entrySet()) {
            String fieldName = entry.getKey();
            JsonElement value = entry.getValue();

            if (value.isJsonPrimitive()) {
                target.add(prefix + fieldName, value);
            }
        }
    }
}
