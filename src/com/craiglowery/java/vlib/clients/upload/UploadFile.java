package com.craiglowery.java.vlib.clients.upload;

import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleStringProperty;

/**
 * Created by Craig on 1/30/2016.
 */
public class UploadFile {
    private final SimpleStringProperty localPath;
    private final SimpleIntegerProperty handle;
    private final SimpleStringProperty title;
    private final SimpleBooleanProperty duplicateCheck;


    public  UploadFile(String localPath, int handle, String title, boolean duplicateCheck) {
        this.localPath=new SimpleStringProperty(localPath);
        this.handle=new SimpleIntegerProperty(handle);
        this.title=new SimpleStringProperty(title);
        this.duplicateCheck = new SimpleBooleanProperty(duplicateCheck);
    }

    public String getLocalPath() {
        return localPath.get();
    }

    public void setLocalPath(String value) {
        localPath.set(value);
    }

    public int getHandle() {
        return handle.get();
    }

    public void setHandle(int value) {
        handle.set(value);
    }

    public String getTitle() {
        return title.get();
    }

    public void setTitle(String value) {
        title.setValue(value);
    }

    public boolean getDuplicateCheck() {
        return duplicateCheck.get();
    }

    public void setDuplicateCheck(boolean value) {
        duplicateCheck.set(value);
    }
}
