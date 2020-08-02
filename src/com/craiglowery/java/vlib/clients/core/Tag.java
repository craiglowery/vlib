package com.craiglowery.java.vlib.clients.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by Craig on 2/2/2016.
 */
public class Tag{

    public enum TagType {SEQUENCE, ENTITY, CATEGORY, NONE};

    public String name;
    public String description;
    public TagType type;
    public String browsingPriority;
    public Set<String> values;

    public Tag(String name, String description, String type, String browsingPriority) {
        this.name = name;
        this.description = description;
        switch (type.toLowerCase()) {
            case "sequence": this.type= TagType.SEQUENCE;
                break;
            case "entity": this.type= TagType.ENTITY;
                break;
            case "category": this.type= TagType.CATEGORY;
                break;
            default:
                this.type= TagType.NONE;
                break;
        }
        this.browsingPriority = browsingPriority;
        this.values=new TreeSet<String>();
    }

    @Override
    public String toString() {
        return name;
    }

    public void addValue(String value) {
        values.add(value);
    }

    public List<String> getValues() {
        return new LinkedList<String>(values);
    }

    public void clearValues() {
        values.clear();
    }

    public boolean isSingular() {
        return values.size()==1;
    }

    public boolean isEmpty() {
        return values.size()==0;
    }

    public boolean isMultiple() {
        return values.size()>1;
    }

}
