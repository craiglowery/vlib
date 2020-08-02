package com.craiglowery.java.vlib.clients.editor;

import javafx.beans.property.LongProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;

/**
 * Maintains informationa bout title changes for a video.
 * Created by craig on 5/21/2017.
 */
public class TitleComparator  {
    private final LongProperty handle;
    private final StringProperty original;
    private final StringProperty revised;
    private final StringProperty flag;

    /**
     * Creates a new title comparator.
     * @param handle The video handle.
     * @param original The original (current) title of the video.
     */
    public TitleComparator(long handle, String original) {
        this.handle = new SimpleLongProperty(handle);
        this.original = new SimpleStringProperty(original);
        this.revised = new SimpleStringProperty(original);
        this.flag=new SimpleStringProperty(" ");
    }

    /**
     * Returns the revised title.
     * @param revised
     */
    public void setRevised(String revised) {
        this.revised.set(revised);
        checkFlag();
    }

    /**
     * Reverts any changes so that revised is again the same as original.
     */
    public void cancelChanges() {
        this.revised.set(original.get());
        checkFlag();
    }

    /**
     * Returns the handle of the video object.
     * @return
     */
    public long getHandle() {
        return handle.get();
    }

    /**
     * Returns the original, unchanged title.
     * @return
     */
    public String getOriginal() {
        return original.get();
    }

    public String getRevised() {
        return original.get().equals(revised.get())?"":revised.get();
    }

    public String getFlag() {
        return flag.get();
    }

    /** Returns true if the title has been changed.
     *
     * @return
     */
    public boolean changed() {
        return !revised.get().equals(original.get());
    }

    private void checkFlag() {
        flag.set(changed()?"*":" ");
    }

    @Override
    public TitleComparator clone() {
        TitleComparator tc = new TitleComparator(handle.get(),original.get());
        tc.setRevised(revised.get());
        return tc;
    }

}
