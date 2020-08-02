package com.craiglowery.java.vlib.clients.editor;

import javafx.collections.ObservableList;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.HBox;
public class IMdBrecord extends HBox {
    String title;
    String year;
    String poster;
    String type;
    String imdbID;
    ImageView iv;
    public IMdBrecord(String title, String year, String poster, String type, String imdbID) {
        super();
        this.title=title;
        this.year=year;
        this.poster=poster;
        this.type=type;
        this.imdbID=imdbID;
        ObservableList<Node> children = getChildren();
        iv=new ImageView();
        try {
            iv.setImage(new Image(poster));
        } catch (Exception e) {
            //Nothing
        }
        int scale=33;
        iv.setFitHeight(3*scale);
        iv.setFitWidth(2*scale);


        children.add(iv);
        children.add(new Label(toString()));
    }
    @Override public String toString() { return String.format("%s (%s - %s)",title,type,year);}
}