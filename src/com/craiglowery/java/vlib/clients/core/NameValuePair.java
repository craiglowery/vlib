package com.craiglowery.java.vlib.clients.core;

/**
 * Created by Craig on 2/4/2016.
 */
public class NameValuePair implements Comparable<NameValuePair>{


    public String name;
    public String value;
    public String display;


    public NameValuePair(String name, String value) {
        this.name=name;
        this.value=value;
        this.display=name+"="+value;
    }
    @Override
    public String toString() {
        return display;
    }


    @Override
    public boolean equals(Object other) {
        if (other==null) return false;
        if (! (other instanceof NameValuePair) ) return false;
        return display.equals(((NameValuePair)other).display);
    }

    @Override
    public int hashCode() {
        return display.hashCode();
    }

    public int compareTo(NameValuePair other) {
        if (other==null) return 1;
        return  display.compareTo(other.display);
    }

    @Override
    public Object clone() {
        return new NameValuePair(name,value);
    }
}
