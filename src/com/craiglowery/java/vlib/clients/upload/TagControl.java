package com.craiglowery.java.vlib.clients.upload;

import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.HBox;

/**
 * Created by Craig on 2/25/2016.
 */
public class TagControl extends HBox {

    Label lbNameValue;
    Button btRemove;
    Button btApplyToAll;

    public TagControl(String tagName, String tagValue) {
        lbNameValue = new Label();
        lbNameValue.setStyle("-fx-font-size: 150%");
        lbNameValue.setText(tagName+"="+tagValue);

        final int j  = 30;

        btRemove = new Button();
        btRemove.setStyle("-fx-background-radius: 5em; -fx-background-color: #ff6666;");
        btRemove.setText("X");
        btRemove.setMinHeight(j);
        btRemove.setMaxHeight(j);
        btRemove.setMinWidth(j);
        btRemove.setMaxWidth(j);


        btApplyToAll = new Button();
        btApplyToAll.setStyle("-fx-background-radius: 5em; -fx-background-color: #66ff66;");
        btApplyToAll.setMinHeight(j);
        btApplyToAll.setMaxHeight(j);
        btApplyToAll.setMinWidth(j);
        btApplyToAll.setMaxWidth(j);
        btApplyToAll.setText("=");

        getChildren().addAll(btRemove,btApplyToAll,lbNameValue);

        setPadding(new Insets(5));
        setSpacing(5);
        setAlignment(Pos.CENTER_LEFT);
    }

}
