package com.skatepark.heelflip.table;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * An utility to get {@link HFValue} from JSON.
 *
 * @author greatjapa
 * @see JsonObject
 */
class Extractor {

    /**
     * Flatter the given JSON.
     *
     * @param id   row id.
     * @param json json object.
     * @return list of values collected.
     */
    public static List<HFValue> extract(UUID id, JsonObject json) {
        List<HFValue> result = new ArrayList<>();
        extract(id, json, result);
        return result;
    }

    /**
     * Flatter the given JSON.
     *
     * @param id        row id.
     * @param json      json object.
     * @param result    value accumulator.
     * @param prefixSeq prefix used in key when we copy to target JSON.
     */
    private static void extract(UUID id, JsonObject json, List<HFValue> result, String... prefixSeq) {
        String prefix = prefixSeq == null ? "" : String.join("", prefixSeq);

        Map<String, JsonObject> objectsMap = new HashMap<>();
        Map<String, JsonArray> arraysMap = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
            String fieldName = prefix + entry.getKey();
            JsonElement value = entry.getValue();

            if (value.isJsonObject()) {
                objectsMap.put(fieldName, value.getAsJsonObject());
            }
            if (value.isJsonArray()) {
                arraysMap.put(fieldName, value.getAsJsonArray());
            }
            if (value.isJsonPrimitive()) {
                result.add(new HFValue(id, fieldName, value.getAsJsonPrimitive()));
            }
        }

        for (Map.Entry<String, JsonObject> entry : objectsMap.entrySet()) {
            String fieldName = entry.getKey();
            JsonObject obj = entry.getValue();

            extract(id, obj, result, fieldName, ".");
        }

        for (Map.Entry<String, JsonArray> entry : arraysMap.entrySet()) {
            String fieldName = entry.getKey();
            JsonArray array = entry.getValue();

            for (int i = 0; i < array.size(); i++) {
                JsonElement elem = array.get(i);
                if (elem.isJsonPrimitive()) {
                    String newFieldName = String.format("%s_%d", fieldName, i);
                    result.add(new HFValue(id, newFieldName, elem.getAsJsonPrimitive()));
                }

                if (elem.isJsonObject()) {
                    extract(id, elem.getAsJsonObject(), result, fieldName, ".");
                }
            }
        }
    }
}
