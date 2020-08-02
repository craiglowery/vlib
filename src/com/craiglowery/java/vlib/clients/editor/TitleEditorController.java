package com.craiglowery.java.vlib.clients.editor;


import com.craiglowery.java.vlib.clients.core.Video;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import javafx.scene.layout.GridPane;

import java.net.URL;
import java.util.ResourceBundle;

/**
 * Created by Craig on 2/3/2016.
 */
public class TitleEditorController implements Initializable {

    @FXML public GridPane gpRoot;
    @FXML public Button btCancel;
    @FXML public Button btOk;
    @FXML public TextField tfTitle;

    Video video;

    @Override
    public void initialize(URL fxmlFileLocation, ResourceBundle resources) {
        btCancel.setCancelButton(true);
        btCancel.setOnAction( event -> {
            btCancel.getScene().getWindow().hide();
        });
        btOk.setDefaultButton(true);
        btOk.setOnAction( event -> {
            update();
            btOk.getScene().getWindow().hide();
        });

    }

    /**
     * Sets the dialog to edit the title attribute for the specified video.
     * @param v Video of interest.
     */
    public void set(Video v) {
        //Remember the parameters and important sizes for later use
        video=v;
        tfTitle.setText(v.getTitle());
    }


    /**
     * <p>Updates the video set with new tag assignments as set in the dialog according to the buttons
     * coverage settings.</p>
     *  <dl>
     *      <dt>ALL</dt>
     *      <dd>Every video will be tagged with the value.</dd>
     *      <dt>SOME</dt>
     *      <dd>Nothing will be changed</dd>
     *      <dt>NONE</dt>
     *      <dd>The value will be removed from any videos on which it currently occurs</dd>
     *  </dl>
     */
    public void update() {
        video.setTitle(tfTitle.getText());
    }

}
