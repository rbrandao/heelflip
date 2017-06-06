package skatepark.heelflip;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * This class provides aggregation info related to an specific JSON field. The aggregations
 * available are: min, max and sum. This object also counts how many values were {@link String},
 * {@link Boolean} or {@link Number} and counts how many times an specific value appeared.
 *
 * @author greatjapa
 */
public class FieldAgg {

    private String fieldName;
    private Map<String, Integer> countMap;

    private int stringCount;
    private int booleanCount;
    private int numberCount;

    private BigDecimal min;
    private BigDecimal max;
    private BigDecimal sum;

    public FieldAgg(String fieldName) {
        Objects.requireNonNull(fieldName, "fieldName should not be null.");
        this.fieldName = fieldName;
        this.countMap = new HashMap<>();
        this.stringCount = 0;
        this.booleanCount = 0;
        this.numberCount = 0;
    }

    public String getFieldName() {
        return fieldName;
    }

    /**
     * @return number of distinct values.
     */
    public int cardinality() {
        return countMap.keySet().size();
    }

    /**
     * @return number of fields.
     */
    public int count() {
        return countMap.values().stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    /**
     * @return number of occurrences of the given value.
     */
    public int count(String value) {
        return value == null || !countMap.containsKey(value) ? 0 : countMap.get(value);
    }

    /**
     * @return set of distinct values.
     */
    public Set<String> distinctValues() {
        return countMap.keySet();
    }

    /**
     * @return count of {@link String} value.
     */
    public int getStringCount() {
        return stringCount;
    }

    /**
     * @return count of {@link Boolean} value.
     */
    public int getBooleanCount() {
        return booleanCount;
    }

    /**
     * @return count of {@link Number} value.
     */
    public int getNumberCount() {
        return numberCount;
    }

    /**
     * @return max value.
     */
    public BigDecimal getMax() {
        return max;
    }

    /**
     * @return min value.
     */
    public BigDecimal getMin() {
        return min;
    }

    /**
     * @return sum value.
     */
    public BigDecimal getSum() {
        return sum;
    }

    void agg(JsonPrimitive value) {
        countMap.computeIfAbsent(value.getAsString(), key -> 0);
        countMap.put(value.getAsString(), countMap.get(value.getAsString()) + 1);

        if (value.isNumber()) {
            numberCount++;

            BigDecimal v = value.getAsBigDecimal();
            min = min == null || min.compareTo(v) > 0 ? v : min;
            max = max == null || max.compareTo(v) < 0 ? v : max;
            sum = sum == null ? v : sum.add(v);
        } else if (value.isString()) {
            stringCount++;
        } else if (value.isBoolean()) {
            booleanCount++;
        }
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
        JsonObject json = new JsonObject();
        json.addProperty("fieldName", fieldName);
        json.addProperty("count", count());
        json.addProperty("cardinality", cardinality());
        json.addProperty("string", stringCount);
        json.addProperty("boolean", booleanCount);
        json.addProperty("number", numberCount);
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
