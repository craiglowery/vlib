package com.craiglowery.java.jobmgr;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

/**
 *
 * <p>A {@code JobManager} object is a priority control queue for managing a group of asynchronously
 * executing threads.  Each thread is wrapped in a {@link Job} object.  The job manager maintains
 * the queue of jobs and runs them in the background. The job manager can run more than one job
 * simultaneously, up to a number (a cap, or limit) known as the &quot;run depth.&quot</p>
 *
 * <p>This object is thread safe, and its methods can be invoked safely from other objects on other threads without
 * additional synchronization other than the method call.</p>
 *
 * <p>Jobs in progress have a {@code T_STATUS} object that is the latest status reported by the job.  A callback
 * {@code Consumer<Job<T_RESULT,T_STATUS>} method can be defined for the job manager and will be invoked
 * whenever the status of a job changes.</p>
 *
 * Jobs that have terminated with no error will have a T_RESULT object which is the result of the
 * job.
 *
 * @See Job
 */
public class JobManager<T_RESULT, T_STATUS> implements Iterable<Job>
{

    //----CONSTRUCTORS-------------------------------------------------------

    /**
     * Constructs a new JobManager and kicks off the scheduler thread.
     * @param statusCallback Called by the job manager whenever a job signals a status change. The job
     *                       object is passed back and can be examined for the status information
     *                       using {@code getLastStatus().  A value of null specifies that no callback
     *                       should be attempted.}.
     **/
    public JobManager(Consumer<Job<T_RESULT, T_STATUS>> statusCallback)
    {
        construct(statusCallback,System.err);
    }

    /**
     * Constructs a new JobManager logging to the specified stream and kicks off the scheduler thread.
     * @param statusCallback Called by the job manager whenever a job signals a status change. The job
     *                       object is passed back and can be examined for the status information
     *                       using {@code getLastStatus().  A value of null specifies that no callback
     *                       should be attempted.}.
     * @param logStream The print stream to log to.
     **/
    public JobManager(Consumer<Job<T_RESULT, T_STATUS>> statusCallback, PrintStream logStream)
    {
        construct(statusCallback,logStream);
    }

    private void construct(Consumer<Job<T_RESULT, T_STATUS>> statusCallback, PrintStream logStream) {
        jobManagerThread = new Thread(() -> scheduler());  //Start the scheduler on another thread
        this.statusCallback = statusCallback;
        jobManagerThread.start();
        log(VerbosityLevel.TRANSITIONS, "Job manager constructed");
        this.logStream = logStream;

    }

    //----PRIVATE VARIABLES------------------------------
    /** A map of jobs by ID **/
    private Map<Integer,Job> jobs = new HashMap<>();

    /** A list of jobs ordered by ID **/
    private Queue<Job> jobQueue = new PriorityQueue<>();

    /** Threads associated with running or finished jobs **/
    private Map<Job,Thread> threads = new HashMap<>();

    /** The ID for the next job to walk in the door **/
    private int nextJobId = 1;

    /** The reference to the JobManager thread, which is essentially a service thread
     * created when the JobManager object is constructed.
     */
    private Thread jobManagerThread=null;

    /**
     * Object to notifyAll on when the job manager has completed stopQueue.
     */
    private Object uponQuiesceNotify =null;

    private Consumer<Job<T_RESULT,T_STATUS>> statusCallback;

    /** If true, the job manager will exit when quiesced or when it has no jobs to execute **/
    private ExitConditions exitCondition = ExitConditions.DO_NOT_EXIT;

    //----QUEUE STATUS-------------------------------------------
    /** Describes the current state of the job queue **/
    public enum QueueStatus
    {
        /** Running normally **/ RUNNING,
        /** Waiting to shut down**/ SHUTTINGDOWN,
        /** All running jobs have stopped **/ QUIESCED,
        /** The job manager is no longer running **/ EXITED
    };

    /** Returns the current state of the job queue **/
    public QueueStatus getQueueStatus()
    {
        return queueStatus;
    }

    /** The current state of the job queue **/
    private QueueStatus queueStatus= QueueStatus.RUNNING;

    //---VERBOSITY LEVEL----------------------------------------

    /**
     * Filters what log entries requested will actually get printed to the log.
     * The most verbose level is EVERYTHING.  Each level subsequent in the enum is
     * less verbose.  When a message is logged, its verbosity level must match or
     * bo the the right of what matches in this list in order to be printed to the
     * log.
     */
    public enum VerbosityLevel { EVERYTHING, INFORMATION, TRANSITIONS, ERRORS};

    public VerbosityLevel getVerbosityLevel() {
        return verbosityLevel;
    }

    public void setVerbosityLevel(VerbosityLevel verbosityLevel) {
        this.verbosityLevel = verbosityLevel;
    }

    private VerbosityLevel verbosityLevel = VerbosityLevel.TRANSITIONS;

    //---RUN DEPTH----------------------------------------------
    /** Returns the current run depth (maximum number of concurrent jobs) **/
    public int getRunDepth()
    {
        return runDepth;
    }

    /**
     * Sets the run depth (maximum number of concurrent jobs)
     * @param value The new value for the run depth. Must be greater than zero.
     * @throws IllegalArgumentException if the value is less than 1.
     */
    public void setRunDepth(int value) throws IllegalArgumentException {
        if (value < 1)
            throw new IllegalArgumentException("Value must be greater than 0");
        runDepth = value;
        log(VerbosityLevel.TRANSITIONS,"Run depth set to %d",value);
        synchronized (this) {
            notifyAll();
        }
    }

    /** The number of jobs that are allowed to run concurrently (depth of run pool) **/
    private int runDepth=1;

    //----EXIT CONDITION

    public enum ExitConditions
    {
        /** No request to exit */ DO_NOT_EXIT,
        /** Nothing to run **/    EXIT_WHEN_IDLE,
        /** Queue stopped **/     EXIT_WHEN_QUIESCED,
        /** Immediate! **/        EXIT_IMMEDIATELY
    };

    /**
     * Sets the scheduler to exit if certain conditions occur, such as becoming idle.
     * @param exitCondition
     */
    public void setExitCondition(ExitConditions exitCondition)
    {
        this.exitCondition = exitCondition;
    }

    //----DEFAULT EXECUTE UPON RETURN --------------------------------------------------------
    /** The default execute upon return code.  Jobs that do not have an execute upon return
     * code set at introduction will have this value assigned instead. The default is null,
     * meaning no default execute upon return code.
     */
    ExecuteUponReturn defaultExecuteUponReturn=null;

    public void setDefaultExecuteUponReturn(ExecuteUponReturn defaultExecuteUponReturn)
    {
        this.defaultExecuteUponReturn=defaultExecuteUponReturn;
    }

    //----LOGGING-----------------------------------------------------
    /** The stream to which the log should be written.  By default, this is System.err. **/
    private PrintStream logStream = System.err;




    //---- PUBLIC JOB CONTROL METHODS -------------------------------------------

    /**
     * Adds a new job to the job queue and returns a job ID.  If there is capacity left in
     * the run poo and the queue is running, the job is started immediately.
     * @param job An abstract instance of a descendent of the {@code job} class, which
     *            encompasses the logic to be executed as the job's task.
     * @return The job ID of the newly created job.
     */
    synchronized public int introduce(Job<T_RESULT,T_STATUS> job) {
        job.setId(nextJobId++);
        log( VerbosityLevel.TRANSITIONS,"Introducing job '%s'" + "as job #"+job.getId(),job.getDescription());
        if (defaultExecuteUponReturn!=null && job.getExecuteUponReturn()==null) {
            job.setExecuteUponReturn(defaultExecuteUponReturn);
            log(VerbosityLevel.INFORMATION,"Job #"+job.getId()+" - assigning default execute upon return function");
        }
        job.setManager(this);
        job.setState(Job.State.INTRODUCED);
        jobs.put(job.getId(),job);
        jobQueue.add(job);
        notifyAll();
        return job.getId();
    }

    /**
     * Returns a summary of the number of jobs in each possible job state.
     * @return A map of {@code Job.Status} values to integer counts.  If
     * there are no jobs in a particular state, that state will NOT be included
     * in the map, and querying it will return a {@code null} value for the {@code Integer}.
     */
    synchronized public Map<Job.State,Integer> getCounts() {
        Map<Job.State,Integer> stats = new HashMap<>();
        synchronized (this) {
            for (Job job : jobQueue) {
                Job.State state = job.getState();
                Integer i = stats.get(state);
                stats.put(state, i == null ? 1 : i + 1);
            }
        }
        return stats;
    }

    /**
     * Returns a string which is suitable to print to a console or log file that shows the
     * current queue state and the list of all jobs in the queue.
     * @return The string with the report content.
     */
    synchronized public String statusReport() {
        log(VerbosityLevel.INFORMATION,"Generating status report");
        StringBuffer sb = new StringBuffer();
        sb.append("Job manager: "+queueStatus+"\n");
        sb.append("Run depth..: "+runDepth+"\n");
        sb.append("JOB ID  PRIORITY  STATUS           DESCRIPTION\n");
        sb.append("------  --------  ---------------  ---------------------------------------\n");
        for (Job job : jobQueue)
            sb.append(String.format("%5d  %8d  %15s  %s\n",job.getId(),job.getPriority(),job.getState(),job.getDescription()));
        return sb.toString();
    }

    /**
     * Places the queue into the {@code SHUTTINGDOWN} state, after which no more jobs will be put into the
     * {@code RUNNING} state.  After all currently running jobs are stopped, the scheduler will perform a
     * {@code notifyWho.notifyAll()} to signal quiescence.
     * @param notifyWho The object which is waiting on a signal that the job queue is quiesced.
     */
    synchronized public void stopQueue(Object notifyWho) {
        log(VerbosityLevel.TRANSITIONS,"Stop queue requested");
        synchronized (this) {
            queueStatus= QueueStatus.SHUTTINGDOWN;
            uponQuiesceNotify =notifyWho;
            notifyAll();
        }
    }

    /**
     * Places the queue into the {@code RUNNING} and forces a re-examination of its contents.
     */
    synchronized public void startQueue() {
        log(VerbosityLevel.TRANSITIONS,"Start queue requested");
        synchronized (this) {
            queueStatus= QueueStatus.RUNNING;
            notifyAll();
        }

    }

    /** Returns the status of a job.
     *
     * @param jobId  The integer ID of the job to query.
     * @return NOSUCHJOB if the job does not exist, otherwise it returns the job's status.
     */
    synchronized public Job.State getStatus(int jobId) {
        Job job = jobs.get(jobId);
        return job==null ? Job.State.NOSUCHJOB : job.getState();
    }

    /**
     * Deletes a job from the queue if it is not in the {@code RUNNING} state.
     * @param jobId The ID of the job to delete.
     * @return True upon success, false if the job is in the {@code RUNNING} state.
     * @throws IllegalArgumentException if the job does not exist.
     */
    synchronized public boolean delete(int jobId) throws IllegalArgumentException {
        log(VerbosityLevel.TRANSITIONS,"Request to delete job %d",jobId);
        Job job =jobs.get(jobId);
        if (job==null)
            throw new IllegalArgumentException(String.format("Job %d not found",jobId));
        if (job.getState().equals(Job.State.RUNNING))
            return false;
        jobQueue.remove(job);
        jobs.remove(jobId);
        return true;
    }

    /** Removes all terminated entries from the queue.**/
    synchronized public void cleanup() {
        log(VerbosityLevel.TRANSITIONS,"Cleanup requested");
        List<Job> targets = new LinkedList<>();
        for (Job job : jobQueue)
            if (job.getState().equals(Job.State.TERMINATED))
                targets.add(job);
        for (Job job:targets) {
            jobQueue.remove(job);
            jobs.remove(job.getId());
            threads.remove(job);
        }
    }

    /**
     * Looks up a job by matching the provided description.  The first job in the queue by job ID that matches the
     * description is returned, or null if one is not found.  Job description uniqueness is not enforced by the
     * Job Manager, so it is up to the user to provide uniqueness if it is important.
     * @param description
     * @return The first job in the queue that matches the description, or null.
     */
    synchronized public Job lookupByDescription(String description) {
        for (Job j : jobQueue)
            if (j.getDescription().equals(description))
                return j;
        return null;
    }
    //---- ITERATOR SUPPORT ---------------------------------------------------------------
    /**
     * An iterator to be used across all jobs in a snapshot of the queue.  The jobs are not filtered based on current state.  All jobs
     * in the queue are visited.
     */
    public class jobIterator implements Iterator<Job> {
        Iterator<Job> snapIterator;
        public jobIterator() {
            snapIterator=new LinkedList<Job>(jobQueue).iterator();
        }
        public void forEachRemaining(Consumer<? super Job> action) {
            snapIterator.forEachRemaining(action);
        }
        public boolean hasNext() {
            return snapIterator.hasNext();
        }
        public Job next() {
            return snapIterator.next();
        }
        public void remove() {
            //No-op
        }
    }

    /**
     * Takes a snapshot of the job queue and returns an iterator over the snapshot.
     * @return Iterator over a snapshot of the job queue.
     */
    synchronized public Iterator<Job> iterator() {
        return new jobIterator();
    }

    //----TO BE CALLED BY JOBS ONLY---------------------------------------------------------------------------

    /**
     * This method is automatically called by a job whenever the job's updateStatus is invoked by its {@link Job#worker}
     * method.
     * @param job
     */
    public void updateStatus(Job<T_RESULT, T_STATUS> job) {
        if (statusCallback!=null)
            statusCallback.accept(job);
    }

    /**
     * This method is automatically called by a job whenever the job ends, either due to an error or because
     * it terminated normally. By default, the job manager executes the return code for the job.
     * @param job
     */
    public void jobEnded(Job<T_RESULT, T_STATUS> job) {
        job.executeReturnCode();
    }


    //---- PROTECTED METHODS

    /**
     * Rebuilds the job queue to ensure that they are in priority order. Usually called by a job
     * after it changes its priority.
     */
    synchronized protected void reorderJobQueue(Runnable intrinsicCode) {
        if (intrinsicCode!=null)     // See if the caller needs us to do something within the confines of the lock
            intrinsicCode.run();
        PriorityQueue<Job> newQueue = new PriorityQueue<>();
        for (Job job : jobQueue)
            newQueue.add(job);    //Can't do this via constructor - won't sort as expected
        jobQueue=newQueue;
    }

    //----------------- PRIVATE METHODS -----------------------------------------

    /**
     * This method runs in a thread that is kicked off by the constructor.
     * It runs in a continuous loop, waiting on this object's intrinsic
     * lock.  Each time it is awakened, it scans the job list and takes action.
     * It stops admitting new jobs to the {@code RUNNING} state if the queue status is
     * not set to {@code RUNNING}, which should be done via the {@link #stopQueue(Object)}  method.
     * After the queue is quiesced, notifyAll() is performed on the object referenced by
     * {@code uponQuiesceNotify}.  The queue can be set to running again by calling the
     * {@link #startQueue()} method.
     *
     */
    private void scheduler() {
        while (true) {
            synchronized (this) {
                log(VerbosityLevel.EVERYTHING,"Scheduler waiting for event");
                try {
                    wait(1000);
                } catch (InterruptedException e) {
                    log(VerbosityLevel.ERRORS,"The scheduler was awakened by an unexpected interrupt");
                }
                if (exitCondition ==ExitConditions.EXIT_IMMEDIATELY) {
                    log(VerbosityLevel.TRANSITIONS,"The scheduler is responding to EXIT_IMMEDIATELY. Exiting.");
                    queueStatus=QueueStatus.EXITED;
                    return;
                }
                log(VerbosityLevel.EVERYTHING,"Scheduler is scanning the queue");
                //System.out.println("--Scheduler tick--");
                //Run the queue here
                //First, a scan to see what's what
                List<Job> waitList = new LinkedList<>();
                int running=0;
                for (Job job : jobQueue) {
                    switch (job.getState()) {
                        case INTRODUCED:
                            log(VerbosityLevel.TRANSITIONS,"Job %d INTRODUCED->WAITING",job.getId());
                            job.setState(Job.State.WAITING);
                        case WAITING:
                            waitList.add(job);
                            break;
                        case LAUNCHING:
                        case RUNNING:
                            running++;
                            break;
                        default:
                    }
                }
                //Are we being asked to stopQueue?
                if (queueStatus== QueueStatus.SHUTTINGDOWN && running==0) {
                    log(VerbosityLevel.TRANSITIONS,"Queue is now quiescent");
                    queueStatus= QueueStatus.QUIESCED;
                    if (exitCondition ==ExitConditions.EXIT_WHEN_QUIESCED) {
                        log(VerbosityLevel.TRANSITIONS,"The scheduler is responding to EXIT_WHEN QUIESCED and exiting.");
                        queueStatus=QueueStatus.EXITED;
                        return;
                    }
                    if (uponQuiesceNotify !=null) {
                            synchronized (uponQuiesceNotify) {
                                uponQuiesceNotify.notifyAll();
                            }
                        uponQuiesceNotify = null;
                    }
                    continue;
                }
                //Is there anything waiting to run?
                while (waitList.size()>0 && queueStatus== QueueStatus.RUNNING && runDepth-running>0) {
                    Job toRun = waitList.remove(0);
                    log(VerbosityLevel.TRANSITIONS,"Launching job %d",toRun.getId());
                    toRun.setState(Job.State.LAUNCHING);
                    running++;
                    if (!kickOff(toRun)) {
                        running--;
                        toRun.setState(Job.State.ERROR);
                        log(VerbosityLevel.ERRORS,"Job %d experienced an error at launch",toRun.getId());
                    } else
                        toRun.setState(Job.State.RUNNING);
                    log(VerbosityLevel.TRANSITIONS,"Job %d running",toRun.getId());
                }

                //If exit on idle is requested...
                if (exitCondition ==ExitConditions.EXIT_WHEN_IDLE && running==0 && waitList.size()==0 ) {
                    log(VerbosityLevel.TRANSITIONS,"The scheduler is responding to EXIT_WHEN IDLE. Exiting.");
                    queueStatus=QueueStatus.EXITED;
                    return;
                }

            } //Synchronized
        } //While true
    }

    /**
     * Creates a thread to run the job's {@code run()} code.  After completion, the job
     * will notify the scheduler that it has finished. (See {@link Job}.)
     * @param job The {@code Job} object to run.
     * @return True if the thread successfully launches.
     */
    private boolean kickOff(Job job) {
        log(VerbosityLevel.TRANSITIONS,"Kicking off job %d",job.getId());
        try {
            Thread thread = new Thread(job);
            threads.put(job,thread);
            thread.start();
        } catch (Exception e) {
            log(VerbosityLevel.ERRORS,"An exception occurred while kicking off job %d - %s",job.getId(),e.getMessage());
            return false;
        }
        return true;
    }

    private SimpleDateFormat logDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss ");

    /**
     * Write's  a log entry, with the current timstamp.
     * @param level The verbosity level.  The message will not be logged if it has a verbosity level
     *              that is more verbose than the one provided.
     * @param format A format string.
     * @param args An argument list to format into the format string.
     */
    public void log(VerbosityLevel level, String format, Object...args) {
        if (level.ordinal()>=verbosityLevel.ordinal()) {
            synchronized (logStream) {
                logStream.print(logDateFormat.format(new Date()));
                logStream.println(String.format(format, args));
                logStream.flush();
            }
        }
    }

}
