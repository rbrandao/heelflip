package com.skatepark.heelflip.table;

import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.util.Objects;
import java.util.UUID;

class HFValue {

    private UUID id;

    private String columnName;

    private JsonPrimitive value;

    public HFValue(UUID id, String columnName, JsonPrimitive value) {
        Objects.requireNonNull(id, "id should not be null.");
        Objects.requireNonNull(columnName, "columnName should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        this.id = id;
        this.columnName = columnName;
        this.value = value;
    }

    public UUID getId() {
        return id;
    }

    public String getColumnName() {
        return columnName;
    }

    public boolean isString() {
        return value.isString();
    }

    public boolean isBoolean() {
        return value.isBoolean();
    }

    public boolean isNumber() {
        return value.isNumber();
    }

    public String getAsString() {
        return value.getAsString();
    }

    public Boolean getAsBoolean() {
        return value.isBoolean() ? value.getAsBoolean() : null;
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

    public Double getAsDouble() {
        try {
            return value.getAsDouble();
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public BigDecimal getAsBigDecimal() {
        return value.getAsBigDecimal();
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
