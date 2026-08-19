package org.project.model;

public enum Environment {
    DEV("dev"),
    TEST("test"),
    STAGING("staging"),
    PROD("prod");

    private final String value;

    Environment(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}