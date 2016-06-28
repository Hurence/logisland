package com.caseystella.analytics.distribution.config;

import com.caseystella.analytics.distribution.config.Type;
import com.caseystella.analytics.distribution.config.Unit;

import java.io.Serializable;

public class RotationConfig implements Serializable {
    Type type;
    Long amount;
    Unit unit;

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Unit getUnit() {
        return unit;
    }

    public void setUnit(Unit unit) {
        this.unit = unit;
    }
}
