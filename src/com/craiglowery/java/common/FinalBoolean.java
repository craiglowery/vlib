package com.craiglowery.java.common;

public class FinalBoolean {
    private boolean value = false;
    public FinalBoolean(boolean value) {
        this.value=value;
    }
    public void set(boolean value) {
        this.value = value;
    }
    public boolean get() {
        return value;
    }

}
