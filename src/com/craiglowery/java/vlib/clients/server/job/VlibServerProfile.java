package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.vlib.clients.server.connector.ConnectionChooserController;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.Initializable;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class VlibServerProfile {


    public String getName() {
        return name;
    }

    public String name;
    public String baseUrl;
    public String username;
    public String password;

    public static Map<String, VlibServerProfile> profilesMap = null;

    public static void loadPresetProfiles() {
        if (profilesMap==null) {
            profilesMap=new HashMap<>();
            addProfileToDictionary(new VlibServerProfile(
                    "Local Video",
                    "http://192.168.1.14:8080/vlib/api",
                    "vlibrarian",
                    "OlagGan2000"
            ));
        }
    }

    public VlibServerProfile(String name, String baseUrl, String username, String password) {
        this.name = name;
        this.baseUrl = baseUrl;
        this.password = password;
        this.username = username;
    }

    public static void addProfileToDictionary(VlibServerProfile profile) {
        profilesMap.put(profile.getName(),profile);
    }

    @Override
    public String toString() {
        return name;
    }

    public static VlibServerProfile getProfile(String name) {
        if (profilesMap==null)
            loadPresetProfiles();
        return profilesMap.get(name);
    }


}



