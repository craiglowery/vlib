package com.craiglowery.java.vlib.clients.jobtest;

public class KVP {
    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String attribute;
    public String value;
    public KVP(String a, String v) { attribute=a; value=v;}

}
