package com.craiglowery.java.vlib.apps;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.util.logging.Level;

public class TestJobQueue extends Application {

    public static void main(String[] args) {
        launch(args);
    }


    @Override
    public void start(Stage primaryStage) throws Exception {
        try {

            FXMLLoader loader = new FXMLLoader((com.craiglowery.java.vlib.clients.jobtest.JobQueueController.class.getResource("JobQueue.fxml")));
            loader.load();
            primaryStage.setTitle("Job Queue Test");
            primaryStage.setScene(new Scene(loader.getRoot(), 800, 800));
            primaryStage.show();

        } catch (Exception e) {
            java.util.logging.Logger.getLogger(getClass().getName()).log(Level.SEVERE, e!=null?e.getMessage():"unknown reason", e);
        }
    }

    @Override
    public void stop() {

    }
}
