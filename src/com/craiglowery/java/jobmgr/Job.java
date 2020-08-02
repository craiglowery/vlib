package com.craiglowery.java.jobmgr;

/**
 * Created by craig on 2/22/2017.
 */
public class Job implements Comparable<Job>, Runnable {
    /** The states a job can be in */
    public enum Status {
        /** No such job (used for status update by ID) **/ NOSUCHJOB,
        /** Added to the queue, but not yet seen by the scheduler **/ INTRODUCED,
        /** Seen by the scheduler, waiting to run **/ WAITING,
        /** Launched to run **/ LAUNCHING,
        /** Currently running **/ RUNNING,
        /** Terminated abnormally **/ ERROR,
        /** Terminated normally **/ TERMINATED
    };



    /** The main body of the code to run **/
    private BackgroundWorker worker;

    private String description;

    public String getDescription() {
        return description;
    }

    private JobManager manager;

    private int id;

    public int getId() {
        return id;
    }

    private Status status;

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Throwable getError() {
        return error;
    }

    private Throwable error=null;

    public Object getResult() {
        return result;
    }

    private Object result = null;



    public Job(BackgroundWorker worker, int id, String description, JobManager manager) {
        this.worker = worker;
        this.id = id;
        this.description=description;
        this.manager=manager;
        this.status = Status.INTRODUCED;
    }

    public int compareTo(Job other) {
        return Integer.compare(this.id,other.id);
    }

    public void run() {
        manager.log("Job %d is going to work",getId());
        try {
            result=worker.work();
            manager.log("Job %d has completed its work successfully",getId());
        } catch (Throwable t) {
            error=t;
            manager.log("Job %d's work was interrupted - %s",getId(),error.getMessage());
        }
        synchronized (manager) {
            if (error!=null)
                status= Status.ERROR;
            else
                status= Status.TERMINATED;
            manager.log("Job %d is notifying the job manager of work completion",getId());
            manager.notifyAll();
        }
    }

    public String status() {
        StringBuffer sb = new StringBuffer();
        sb.append("Job ID.............: ").append(id).append("\n")
          .append("Job description....: ").append(description).append("\n")
          .append("Error..............: ").append(error==null?"none":error.toString()).append("\n")
          .append("Result.............: ").append(result==null?"none":result.toString()).append("\n")
        ;
        return sb.toString();
    }

}
