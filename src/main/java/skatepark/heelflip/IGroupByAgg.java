package skatepark.heelflip;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.util.Set;


/**
 * This interface provides aggregation info related to an specific JSON field group by other JSON
 * field. The aggregations available are identical to {@link IFieldAgg} but grouped for specific
 * values.
 *
 * @author greatjapa
 * @see IFieldAgg
 */
public interface IGroupByAgg {

    String getFieldName();

    String getGroupBy();

    void agg(JsonPrimitive fieldNameValue, JsonPrimitive groupByValue);

    /**
     * Get set with all values grouped by the given value.
     *
     * @param value value.
     */
    IFieldAgg groupBy(String value);

    /**
     * @return set with all group by values.
     */
    Set<String> groupByValues();

    /**
     * @return set with all values grouped by this aggregation.
     */
    Set<String> values();

    @Override
    String toString();

    /**
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @return String that contains the group by aggregation info in JSON format.
     */
    default String toString(boolean includeValues) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(toJSON(includeValues));
    }

    /**
     * @param includeValues true if needed to write the JSON values related to aggregations, false
     *                      otherwise.
     * @return JsonObject that contains the group by aggregation info.
     */
    default JsonObject toJSON(boolean includeValues) {
        JsonArray values = new JsonArray();
        for (String key : groupByValues()) {
            IFieldAgg fieldAgg = groupBy(key);
            JsonObject obj = new JsonObject();
            obj.add(key, fieldAgg.toJSON(includeValues));
            values.add(obj);
        }

        JsonObject json = new JsonObject();
        json.addProperty("groupBy", getGroupBy());
        json.addProperty("fieldName", getFieldName());
        json.add("values", values);

        return json;
    }

}
