package com.craiglowery.java.vlib.clients.server.connector;

import javafx.concurrent.Task;

/**
 * Created by Craig on 2/2/2016.
 */
public abstract class ServerConnectorTask<T> extends Task<T> {

        public String status(String msg) {
            updateMessage(msg);
            return msg;
        }

        public void progress(long workDone, long max) {
            updateProgress(workDone,max);
        }

}
