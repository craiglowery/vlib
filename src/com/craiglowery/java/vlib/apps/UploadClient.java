package com.craiglowery.java.vlib.apps;


import com.craiglowery.java.vlib.clients.upload.UploadController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.scene.control.Alert;
import javafx.stage.Stage;

import java.util.logging.Level;

/**
 * Created by Craig on 6/10/2016.
 */
public class UploadClient extends Application {

    public static void main(String[] args) {
        launch(args);
    }


    @Override
    public void start(Stage primaryStage) throws Exception {
        try {
            new Alert(Alert.AlertType.CONFIRMATION);
            FXMLLoader loader = new FXMLLoader((UploadController.class.getResource("Upload.fxml")));
            loader.load();
            primaryStage.setTitle("Vlib Object Uploader");
            primaryStage.setScene(new Scene(loader.getRoot(), 300, 275));
            primaryStage.show();

            UploadController controller = loader.getController();
            controller.postStagingInitialization();

        } catch (Exception e) {
            java.util.logging.Logger.getLogger(UploadClient.class.getName()).log(Level.SEVERE, e!=null?e.getMessage():"unknown reason", e);
        }
    }

    @Override
    public void stop() {

    }
}
