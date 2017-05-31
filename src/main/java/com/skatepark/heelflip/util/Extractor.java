package com.skatepark.heelflip.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An utility to get all {@link JsonPrimitive} from {@link JsonObject}.
 *
 * @author greatjapa
 * @see JsonObject
 * @see JsonPrimitive
 */
public class Extractor {

    /**
     * Extract all {@link JsonPrimitive} from the given {@link JsonObject}.
     *
     * @param json json object.
     * @return map of values collected.
     */
    public static Map<String, List<JsonPrimitive>> extract(JsonObject json) {
        Map<String, List<JsonPrimitive>> result = new HashMap<>();
        extract(json, result);
        return result;
    }

    /**
     * Extract all {@link JsonPrimitive} from the given {@link JsonObject} and accumulate their on
     * the given list.
     *
     * @param json      json object.
     * @param result    value accumulator.
     * @param prefixSeq prefix used to named the new key when we navigate to {@link JsonArray} or
     *                  nested {@link JsonObject}.
     */
    private static void extract(JsonObject json, Map<String, List<JsonPrimitive>> result, String... prefixSeq) {
        String prefix = prefixSeq == null ? "" : String.join("", prefixSeq);

        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            String fieldName = prefix + entry.getKey();
            JsonElement value = entry.getValue();

            if (value.isJsonPrimitive()) {
                result.computeIfAbsent(fieldName, key -> new LinkedList<>()).add(value.getAsJsonPrimitive());

            } else if (value.isJsonObject()) {
                extract(value.getAsJsonObject(), result, fieldName, ".");

            } else if (value.isJsonArray()) {
                JsonArray array = value.getAsJsonArray();
                for (int i = 0; i < array.size(); i++) {
                    JsonElement elem = array.get(i);
                    if (elem.isJsonPrimitive()) {
                        String newFieldName = String.format("%s_%d", fieldName, i);
                        result.computeIfAbsent(newFieldName, key -> new LinkedList<>()).add(elem.getAsJsonPrimitive());
                    } else if (elem.isJsonObject()) {
                        extract(elem.getAsJsonObject(), result, fieldName, ".");
                    }
                }
            }
        }
    }
}
