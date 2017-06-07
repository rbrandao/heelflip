package skatepark.heelflip;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.Set;

/**
 * This interface provides aggregation info related to an specific JSON field. The aggregations
 * available are: min, max and sum. This object also counts how many values were {@link String},
 * {@link Boolean} or {@link Number} and counts how many times an specific value appeared.
 *
 * @author greatjapa
 */
public interface IFieldAgg {

    String getFieldName();

    /**
     * @return number of distinct values.
     */
    int cardinality();

    /**
     * @return number of fields.
     */
    int count();

    /**
     * @return number of occurrences of the given value.
     */
    int count(String value);

    /**
     * @return set of distinct values.
     */
    Set<String> distinctValues();

    /**
     * @return count of {@link String} value.
     */
    int getStringCount();

    /**
     * @return count of {@link Boolean} value.
     */
    int getBooleanCount();

    /**
     * @return count of {@link Number} value.
     */
    int getNumberCount();

    /**
     * @return max value.
     */
    BigDecimal getMax();

    /**
     * @return min value.
     */
    BigDecimal getMin();

    /**
     * @return sum value.
     */
    BigDecimal getSum();

    void agg(JsonPrimitive value);

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
        JsonObject json = new JsonObject();
        json.addProperty("fieldName", getFieldName());
        json.addProperty("count", count());
        json.addProperty("cardinality", cardinality());
        json.addProperty("string", getStringCount());
        json.addProperty("boolean", getBooleanCount());
        json.addProperty("number", getNumberCount());

        BigDecimal min = getMin();
        BigDecimal max = getMax();
        BigDecimal sum = getSum();
        if (min != null && max != null & sum != null) {
            json.addProperty("min", min.longValue());
            json.addProperty("max", max.longValue());
            json.addProperty("sum", sum.longValue());
        }
        if (includeValues) {
            JsonArray valuesArray = new JsonArray();
            distinctValues().stream().forEach(valuesArray::add);
            json.add("values", valuesArray);
        }
        return json;
    }
}
