package com.craiglowery.java.vlib.clients.core;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class NameValuePairList extends LinkedList<NameValuePair> {
    /**
     * Determines if there is at least one entry in the list with the specified name.
     * @param name
     * @return
     */
    public boolean isNameDefined(String name) {
        return this.parallelStream().anyMatch(nvp -> nvp.name.equals(name));
    }

    /**
     * Delete's all name value pairs with a specific name.
     * @param name The name to search for and delete from the list.
     */
    public void deleteName(String name) {
       List<NameValuePair> toDelete = this.parallelStream().filter(nvp -> nvp.name.equals(name)).collect(Collectors.toList());
       removeAll(toDelete);
    }

    @Override
    public Object clone() {
        NameValuePairList list = new NameValuePairList();
        this.stream().forEach(nvp -> list.add((NameValuePair)nvp.clone()));
        return list;
    }

}
