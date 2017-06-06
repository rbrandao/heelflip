package skatepark.heelflip;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * This class provides aggregation info related to an specific JSON field group by other JSON field.
 * The aggregations available are identical to {@link FieldAgg} but grouped for specific values.
 *
 * @author greatjapa
 * @see FieldAgg
 */
public class GroupByAgg {

    private String fieldName;

    private String groupBy;

    private Map<String, FieldAgg> aggregations;

    public GroupByAgg(String fieldName, String groupBy) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        Objects.requireNonNull(groupBy, "groupBy should not be null.");
        if (fieldName.equals(groupBy)) {
            throw new IllegalArgumentException("fieldName should not be equal to groupBy.");
        }
        this.fieldName = fieldName;
        this.groupBy = groupBy;
        this.aggregations = new HashMap<>();
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getGroupBy() {
        return groupBy;
    }

    void agg(JsonPrimitive fieldNameValue, JsonPrimitive groupByValue) {
        FieldAgg fieldAgg = aggregations.computeIfAbsent(groupByValue.getAsString(), key -> new FieldAgg(fieldName));
        fieldAgg.agg(fieldNameValue);
    }

    /**
     * Get set with all values grouped by the given value.
     *
     * @param value value.
     */
    public FieldAgg groupBy(String value) {
        return value == null || !aggregations.containsKey(value) ?
                null :
                aggregations.get(value);
    }

    /**
     * @return set with all group by values.
     */
    public Set<String> groupByValues() {
        return aggregations.keySet();
    }

    /**
     * @return set with all values grouped by this aggregation.
     */
    public Set<String> values() {
        return aggregations.values().stream()
                .flatMap(fieldAgg -> fieldAgg.distinctValues().stream())
                .collect(Collectors.toSet());
    }

    @Override
    public String toString() {
        return toString(false);
    }

    /**
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @return String that contains the group by aggregation info in JSON format.
     */
    public String toString(boolean includeValues) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(toJSON(includeValues));
    }

    /**
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @return JsonObject that contains the group by aggregation info.
     */
    public JsonObject toJSON(boolean includeValues) {
        JsonArray values = new JsonArray();
        for (Map.Entry<String, FieldAgg> entry : aggregations.entrySet()) {
            JsonObject obj = new JsonObject();
            obj.add(entry.getKey(), entry.getValue().toJSON(includeValues));
            values.add(obj);
        }

        JsonObject json = new JsonObject();
        json.addProperty("groupBy", groupBy);
        json.addProperty("fieldName", fieldName);
        json.add("values", values);

        return json;
    }
}
