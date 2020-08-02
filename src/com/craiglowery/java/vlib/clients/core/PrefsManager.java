package com.craiglowery.java.vlib.clients.core;

/**
 * Created by craig on 8/31/2016.
 */


import com.craiglowery.java.common.Util;
import javafx.animation.KeyFrame;
import javafx.animation.Timeline;
import javafx.event.EventHandler;
import javafx.scene.Node;
import javafx.stage.Window;
import javafx.stage.WindowEvent;
import javafx.util.Duration;

import java.util.SortedSet;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;

/**
 * Provides support to automatically remember window positions and perform persistence operations on startup or quiesce
 */
public class PrefsManager {

    private Preferences prefs;

    public PrefsManager(Object controller, Node sceneReferenceNode, EventHandler<WindowEvent> handleCloseRequest) {
        prefs = Preferences.userRoot().node(controller.getClass().getPackage().getName());

        Timeline tl = new Timeline();
        KeyFrame kf = new KeyFrame(Duration.millis(1000),event ->manageWindowPosition(sceneReferenceNode,handleCloseRequest));
        tl.getKeyFrames().add(kf);
        tl.setAutoReverse(false);
        tl.setCycleCount(1);
        tl.play();
    }

    public Preferences getPreferences() {
        return prefs;
    }

    /**
     * Positions the window associated with a JavaFX Node to the location stored in preferences,
     * if such information exists in preferences.  Sets up listeners to observe the window's
     * future position and update preferences if they change.
     *
     * @param node The node which references the JavaFX scene, the window of which is to be
     *             managed.
     */
    private void manageWindowPosition(Node node, EventHandler<WindowEvent> handleCloseRequest) {
        Window window = node.getScene().getWindow();

        Util.parseDouble(prefs.get("WindowWidth",null)).ifPresent(window::setWidth);
        Util.parseDouble(prefs.get("WindowHeight",null)).ifPresent(window::setHeight);
        Util.parseDouble(prefs.get("WindowX",null)).ifPresent(window::setX);
        Util.parseDouble(prefs.get("WindowY",null)).ifPresent(window::setY);

        window.heightProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowHeight", String.valueOf(newValue)));
        window.widthProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowWidth", String.valueOf(newValue)));
        window.xProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowX", String.valueOf(newValue)));
        window.yProperty().addListener((observable, oldValue, newValue) -> prefs.put("WindowY", String.valueOf(newValue)));

        if (handleCloseRequest!=null)
            window.setOnCloseRequest(handleCloseRequest);
    }

    public void saveArray(String[] list, String name) {
        Preferences node = prefs.node(name);
        try {
            node.clear();
            for (int x=0; x<list.length; x++) {
                String key = String.format("%05d",x);
                node.put(key,list[x]);
            }
            node.flush();
        } catch (BackingStoreException e) {
            System.err.println("Preferences backing store exception "+e.getMessage());
        }


    }

    /**
     * Loads an array of strings from the preferences context, ordering them as they were when they were
     * originally stored.
     * @param name
     * @return
     */
    public String[] loadArray(String name) {
        try {
            Preferences node = prefs.node(name);
            SortedSet<String> keys = new java.util.TreeSet<String>();
            for (String k : node.keys())
                keys.add(k);
            String[] returnValue = new String[keys.size()];
            Preferences pairsPrefs = prefs.node(name);
            int x=0;
            for (String idx : keys) {
                returnValue[x++] = pairsPrefs.get(idx, null);
            }
            return returnValue;
        } catch (BackingStoreException e) {
            System.err.println("Preferences backing store exception "+e.getMessage());            return new String[] {};
        }
    }

}
