package com.skatepark.heelflip.table;

import com.google.gson.JsonPrimitive;

import java.util.Objects;
import java.util.UUID;

class HFValue {

    private UUID id;

    private JsonPrimitive value;

    public HFValue(UUID id, JsonPrimitive value) {
        Objects.requireNonNull(id, "id should not be null.");
        Objects.requireNonNull(value, "value should not be null.");

        this.id = id;
        this.value = value;
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
}
