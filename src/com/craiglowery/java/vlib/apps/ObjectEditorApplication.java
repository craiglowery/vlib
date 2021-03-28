package com.craiglowery.java.vlib.apps;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.util.logging.Level;

/**
 * Created by Craig on 6/10/2016.
 */
public class ObjectEditorApplication extends Application {

    public static void main(String[] args) {
        launch(args);
    }


    @Override
    public void start(Stage primaryStage) throws Exception {
        try {

            FXMLLoader loader = new FXMLLoader((com.craiglowery.java.vlib.clients.editor.ObjectEditorController.class.getResource("ObjectEditor.fxml")));
            loader.load();
            primaryStage.setTitle("Vlib Object Editor");
            primaryStage.setScene(new Scene(loader.getRoot(), 300, 275));
            primaryStage.show();

        } catch (Exception e) {
            java.util.logging.Logger.getLogger(getClass().getName()).log(Level.SEVERE, e!=null?e.getMessage():"unknown reason", e);
        }
    }

    @Override
    public void stop() {

    }
}
