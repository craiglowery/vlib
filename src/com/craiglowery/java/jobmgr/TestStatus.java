package com.craiglowery.java.jobmgr;


import javafx.collections.MapChangeListener;
import javafx.collections.ObservableMap;
import javafx.collections.FXCollections;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class TestStatus extends Job<String, String> {


    public TestStatus(String description, ExecuteUponReturn returnCode) {
        super(description,returnCode);
    }
    @Override
    protected String worker() {

        ObservableMap<String,Object> status = FXCollections.observableHashMap();

        Object listenerLock = new Object();

        MapChangeListener<String,Object> cl = new MapChangeListener<String, Object>() {
            @Override
            public void onChanged(Change<? extends String, ? extends Object> change) {
                synchronized (listenerLock) {
                    updateStatus(
                            String.format("CHANGE----------------------\n" +
                                            "   Key..........: %s\n" +
                                            "   Value........: %s\n" +
                                            "   Change.......: %s\n",
                                    change.getKey(),
                                    change.getMap().get(change.getKey()),
                                    (change.wasAdded() ? "Added" : (change.wasRemoved() ? "Removed" : "Modified")))
                    );
                }
            }
        };

        status.addListener(cl);

        String tags[] = new String[]{ "Alpha","Beta","Gamma","Delta","Epsilon","Theta"};

        for (int x=1; x<20; x++) {
            try {
                Thread.sleep(rint(3000));
            } catch (Exception e) {
            }

            //Get a random tag
            String tag = tags[rint(tags.length)];
            Integer value = rint(Integer.MAX_VALUE);

            if (rint(100)>50)
                status.put(tag,value);
            else if (status.containsKey((tag)))
                status.remove(tag);
            else status.put(tag,value);
        }

        return "--ALL DONE--";
    }

    private int rint(int max) {
        return (int)(Math.random()*max);
    }

}


