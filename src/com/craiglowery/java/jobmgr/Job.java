package com.craiglowery.java.jobmgr;

/**
 * <p>A {@code Job} object is a {@link Thread} wrapper that adds job control functionality when used with a
 * {@link JobManager} object.  This is a generic abstract class that must be subclassed to implement
 * the {@link Job#worker()} method.</p>
 *
 * <p>{@code T_RESULT} is the class type that will be returned by the thread as a result. </p>
 *
 * <p>{@code T_STATUS} is the class type that may optionally be used intermittently by the thread to indicate to the
 * {@link JobManager} that something has changed which is of interest to outside observers.</p>
 *
 * <p>The {@link Job#worker()} implementation is the logic that will be run in the thread, and return a
 * {@code T_RESULT}.  The thread can
 * optionally send status updates to the {@link JobManager} by invoking the {@link #updateStatus(T_STATUS)}
 * method.</p>
 *
 * <p>The {@link JobManager} object that owns this job can be referenced with a call to {@link #getManager()}.</p>
 *
 * <p>{@link #worker()} may throw an {@link Exception} to indicate an error state.  In this case, the job
 * manager will place the exception object into the job's {@code error} attribute before the job
 * is terminated.  </p>
 *
 * <p>Observers of the job may query a terminated job's state via {@link #getState()} to determine if a job
 * terminated normally or abnormally, and then access either the result ({@link #getJobResult()}) or the
 * exception object ({@link #getError()}) accordingly.</p>
 *
 * <p>This class is thread-safe for all public setter and getter methods.  Observers
 * can call these methods without worrying about synchronization issues between threads.
 * However, the objects returned by the getters should not be modified by observers as they are not
 * guaranteed to be thread-safe. Furthermore, there is no guarantee that the values of the
 * objects returned (like a job's status) have not been modified since the update change was
 * signalled. The {@link #run()} and {@link #worker()} methods are not
 * thread-safe and should only be called by the associated {@link JobManager}.</p>
 *
 * <p> As a default behavior, when job's {@link #worker} method ends and returns the {@code T_RESULT}
 * object, the {@link JobManager} will do nothing more than update the information in the job queue. It
 * it is up to the client of the job manager to observe the status change and act on it by
 * calling {@link #getJobResult()}.</p>
 *
 * <p>But {@link Job}s can be more proactive in signalling the client upon their termination, whether
 * due to error or normal return, by setting an {@link ExecuteUponReturn} function.  The {@link JobManager}
 * can also be configured to have a default {@link ExecuteUponReturn} function.  In either case, if the
 * job has such a function assigned (not null) then it will be executed when the job exits.  Most clients
 * will specify {@link ExecuteUponReturn} functions that implement thread-safe asynchronous communication
 * from the {@link JobManager}'s scheduler thread (where the function is executed) to the client's thread.
 * A typical method is to use a {@link java.util.concurrent.BlockingQueue} or similar mechanism.</p>
 *
 * @param <T_RESULT> The class type of the object to be returned by the {@link #worker()} method, or null
 *                   if no result will be returned.
 * @param <T_STATUS> The class type of the object to be used in communicated status updates to observers
 *                   via the {@link #updateStatus(T_STATUS)} method, or null if no status updates will
 *                   be performed.  Observers will usually obtain a reference to the job from the {@link JobManager}
 *                   and then use the job's {@link #getLastStatus()} method to obtain the status.
 * @author Craig Lowery
 * @see JobManager
 */
abstract public class Job<T_RESULT, T_STATUS> implements Comparable<Job<T_RESULT, T_STATUS>>, Runnable {


    //----CONSTRUCTORS------------------------------------------------------------------------------------

    /**
     * Constructs a new job.  The client should use these constructors of a concrete implementation
     * of a descendent class when submitting a job to the job manager.
     *
     * @param description       A string description of the job.
     * @param executeUponReturn Function the {@link JobManager} should execute when this job ends, or
     *                          null if there is no such function.
     */
    public Job(String description, ExecuteUponReturn executeUponReturn) {
        this.executeUponReturn = executeUponReturn;
        this.description = description;
    }

    /**
     * Contructs a new job without specifying an {@link ExecuteUponReturn} function.
     *
     * @param description The job's description string.
     */
    public Job(String description) {
        this.description = description;
    }

    //----ABSTRACT METHODS THAT MUST BE INSTANTIATED-----------------------------------------------------

    /**
     * This is an abstract method that must be implemented in derived classes. It will be called on the
     * job's thread to do whatever work in the background is required, then return a {@code T_RESULT}.
     *
     * @return The {@code T_RESULT} value to pass back to the {@link JobManager}.
     * @throws Exception The worker may throw an exception depending on the implementatnoi.
     */
    protected abstract T_RESULT worker() throws Exception;


    //----"EXECUTE UPON RETURN" FUNCTION-----------------------------------------------------------------
    /**
     * The optional code to be executed by the client upon return by calling {@link #executeReturnCode}.
     */
    private ExecuteUponReturn executeUponReturn = null;

    /**
     * Sets the function to be executed by the {@link JobManager} when this job ends.
     *
     * @param executeUponReturn The function to execute, or {@code null} if no function should be executed.
     */
    public void setExecuteUponReturn(ExecuteUponReturn executeUponReturn) {
        this.executeUponReturn = executeUponReturn;
    }

    public ExecuteUponReturn getExecuteUponReturn()
    {
        return executeUponReturn;
    }

    /**
     * Executes the job's executeUponReturn function, if one has been specified.
     */
    public void executeReturnCode() {
        if (executeUponReturn != null)
            executeUponReturn.acceptCompletedJob(this);
    }


    //----JOB DESCRIPTION--------------------------------------------

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * The job's description string
     **/
    private String description;

    //----JOB MANAGER link -------------------------------------------

    public JobManager<T_RESULT, T_STATUS> getManager() {
        return manager;
    }

    public void setManager(JobManager<T_RESULT, T_STATUS> manager) {
        this.manager = manager;
    }

    /**
     * A reference to the job manager object (queue) managing this job
     **/
    JobManager<T_RESULT, T_STATUS> manager;

    //----JOB ID---------------------------------------------------

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    /**
     * The job's unique ID within its associated job manager
     **/
    private int id;

    //----JOB STATE-------------------------------------------

    /**
     * The states a job can be in
     */
    public enum State {
        /**
         * No such job (used when the job manager is asked about a job by ID, but there is no job with that ID)
         **/
        NOSUCHJOB,
        /**
         * Added to the queue, but not yet seen by the scheduler
         **/
        INTRODUCED,
        /**
         * Seen by the scheduler, waiting to run
         **/
        WAITING,
        /**
         * Launched to run
         **/
        LAUNCHING,
        /**
         * Currently running
         **/
        RUNNING,
        /**
         * Terminated abnormally
         **/
        ERROR,
        /**
         * Terminated normally
         **/
        TERMINATED
    }


    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    /**
     * The job's current state in the queue
     **/
    private State state;

    //----ERROR (LAST EXCEPTION THROWN)-------------------------------------------

    public Throwable getError() {
        return error;
    }

    public void setError(Throwable error) {
        this.error = error;
    }

    /**
     * A reference to the most recently thrown exception from the background worker
     **/
    private Throwable error = null;

    //----T_RESULT-------------------------------------------

    /**
     * Used to get the job's result.
     *
     * @return
     */
    public T_RESULT getJobResult() {
        return result;
    }

    /**
     * Set's the job result. Usually called by the {@link JobManager}.
     *
     * @param result
     */
    public void setResult(T_RESULT result) {
        this.result = result;
    }

    /**
     * A reference to the result object returned by the background worker
     **/
    private T_RESULT result = null;

    //----T_STATUS-------------------------------------------

    /**
     * Returns the most recent job status.  Note the job "status" is different than
     * job "state."  Job "state" is relevant to is state in the job manager's
     * scheduler.  Job "status" is defined by the creator of the subclass of the job.
     *
     * @return
     */
    public T_STATUS getLastStatus() {
        return lastStatus;
    }

    /**
     * Set's the job's status. Usually called by the {@link #worker} method.
     *
     * @param lastStatus
     */
    public void setLastStatus(T_STATUS lastStatus) {
        this.lastStatus = lastStatus;
    }

    /**
     * Hold's the job's most recent status
     **/
    private T_STATUS lastStatus = null;


    //----PRIORITY-------------------------------------------

    /**
     * Returns the job's priority.
     *
     * @return
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Changes the job's priority and asks the job manager to reorder to the queue appropriately.
     *
     * @param priority A non-negative integer between 1 and 100.  Lower numbers are higher priorities.
     */
    public void setPriority(int priority) throws Exception {
        if (priority < 1 || priority > 100)
            throw new Exception("Priority " + priority + " invalid. Must be in the range 1..100");
        if (manager != null) {
            manager.reorderJobQueue(() -> this.priority = priority);
        } else
            this.priority = priority;

    }

    /**
     * The job's priority
     **/
    private int priority = 1;

    //----UTILITY FUNCTIONS-------------------------------------------

    /**
     * Compares one job's priority and ID to another job's priority ID. Used for sorting
     * in the priority queue in the job manager.
     *
     * @param other
     * @return
     */
    public int compareTo(Job<T_RESULT, T_STATUS> other) {
        long me = this.id + this.priority * 1000000;
        long them = other.id + other.priority * 1000000;
        return Long.compare(me, them);
    }

    /**
     * Initiates the job's background worker.  run() should be executed on a separate thread, as it
     * blocks until the background worker completes.  The {@link JobManager} takes care of creating
     * a separate thread, launching it, and then having that thread's {@link Thread#start} method call
     * this method.  This method run's the {@link #worker} method, capture's the result, determines
     * if an error occured, and configures the {@link JobManager} with this information.
     */
    public void run() {
        manager.log(JobManager.VerbosityLevel.TRANSITIONS,"Job %d is going to work", getId());
        try {
            setResult(worker());
            manager.log(JobManager.VerbosityLevel.TRANSITIONS,"Job %d has completed its work successfully", getId());
        } catch (Throwable t) {
            setError(t);
            manager.log(JobManager.VerbosityLevel.ERRORS,"Job %d's work was interrupted - %s", getId(), error.getMessage());
        }
        synchronized (manager) {
            if (error != null)
                setState(State.ERROR);
            else
                setState(State.TERMINATED);
            manager.jobEnded(this);
            manager.log(JobManager.VerbosityLevel.TRANSITIONS,"Job %d is notifying the job manager of work completion", getId());
            manager.notifyAll();
        }
    }

    /**
     * Called by the {@link #worker} method to indicate a status update, which is communicated
     * back to the {@link JobManager}.
     *
     * @param status
     */
    public void updateStatus(T_STATUS status) {
        lastStatus = status;
        manager.updateStatus(this);
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb
                .append("JOB ID..........: ").append(id).append("\n")
                .append("DESCRIPTION.....: ").append(description).append("\n")
                .append("STATE...........: ").append(state.toString()).append("\n").append("\n");
        return sb.toString();
    }

}
