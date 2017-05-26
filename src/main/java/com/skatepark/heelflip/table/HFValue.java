package com.skatepark.heelflip.table;

import com.google.gson.JsonPrimitive;

import java.util.Objects;
import java.util.UUID;

class HFValue {

    private String columnName;

    private UUID id;

    private JsonPrimitive value;

    public HFValue(String columnName, UUID id, JsonPrimitive value) {
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(id, "id should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        this.columnName = columnName;
        this.id = id;
        this.value = value;
    }

    public String getColumnName() {
        return columnName;
    }

    public UUID getId() {
        return id;
    }

    public Double getAsDouble() {
        try {
            return value.getAsDouble();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Integer getAsInt() {
        try {
            return value.getAsInt();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Long getAsLong() {
        try {
            return value.getAsLong();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public String getAsString() {
        return value.getAsString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HFValue hfValue = (HFValue) o;

        if (!columnName.equals(hfValue.columnName)) return false;
        if (!id.equals(hfValue.id)) return false;
        return value.equals(hfValue.value);

    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + id.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s:%s (%s)", columnName, value, id);
    }
}
