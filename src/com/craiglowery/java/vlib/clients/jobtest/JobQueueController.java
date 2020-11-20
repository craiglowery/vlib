package com.craiglowery.java.vlib.clients.jobtest;

import com.craiglowery.java.jobmgr.ExecuteUponReturn;
import com.craiglowery.java.jobmgr.Job;
import com.craiglowery.java.jobmgr.JobManager;
import com.craiglowery.java.vlib.clients.server.job.VlibServerJob_UploadObject;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.Initializable;
import javafx.scene.control.TableView;

import java.net.URL;
import java.util.ResourceBundle;
import java.util.function.Consumer;

public class JobQueueController implements Initializable {

    private final ObservableList<VlibServerJob_UploadObject> vlibServerJobs = FXCollections.observableArrayList();

    TableView jobsTableView = null;



    public void initialize(URL url, ResourceBundle resourceBundle) {
        JobManager<String,String> jm = new JobManager<>(
                /* Status callback */
                new Consumer<Job<String, String>>() {
                    @Override
                    public void accept(Job<String, String> job) {
                        Platform.runLater( null );
                    }
                }
        );

        jm.setRunDepth(2);

        ExecuteUponReturn returnCode = new ExecuteUponReturn() {
            @Override
            public void acceptCompletedJob(Job job) {
                Platform.runLater( null );
            }
        };

        //jobsTableView = new

    }


}
