package com.craiglowery.java.jobmgr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

/**
 * Created by craig on 2/22/2017.
 */
public class JobManager implements Iterable<Job>{

    //=------------ PUBLIC ENUMS -------------------------------

    /** Describes the current state of the job queue **/
    public enum QueueStatus {
        /** Running normally **/ RUNNING,
        /** Waiting to shut down**/ SHUTTINGDOWN,
        /** All running jobs have stopped **/ QUIESCED};

    //------------- PRIVATE ATTRIBUTES --------------------------

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

    /** The number of jobs that are allowed to run concurrently (depth of run pool) **/
    private int runDepth=5;


    /** The current state of the job queue **/
    private QueueStatus queueStatus= QueueStatus.RUNNING;

    //-------------- CONSTRUCTOR ----------------------------------------------------

    /** Constructs a new JobManager and kicks off the scheduler thread. **/
    public JobManager(int runDepth) {
        this.runDepth=runDepth;
        jobManagerThread = new Thread(() -> scheduler());
        jobManagerThread.start();
        log("Job manager constructed");
    }

    //-------------- GETTERS AND SETTERS -------------------------------------------

    /** Returns the current state of the job queue **/
    public QueueStatus getQueueStatus() {
        return queueStatus;
    }

    /** Returns the current run depth (maximum number of concurrent jobs) **/
    public int getRunDepth() {
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
        log("Run depth set to %d",value);
        synchronized (this) {
            notifyAll();
        }
    }


    //---------------- PUBLIC METHODS -------------------------------------------

    /**
     * Adds a new job to the job queue and returns a job ID.  If there is capacity left in
     * the run pool, the job is started immediately.
     * @param description A string value to use a description in the job listing.
     * @param worker The work to be done by the job when it is run.
     * @return The job ID of the newly created job.
     */
    synchronized public int introduce(String description,BackgroundWorker worker) {
        log("Introducing job %s",description);
        Job job = new Job(worker, nextJobId, description, this);
        jobs.put(nextJobId,job);
        jobQueue.add(job);

        notifyAll();
        return nextJobId++;
    }

    /**
     * Returns a summary of the number of jobs in each possible job state.
     * @return A map of {@code Job.Status} values to integer counts.  If
     * there are no jobs in a particular state, that state will NOT be included
     * in the map, and querying it will return a {@code null} value for the {@code Integer}.
     */
    public Map<Job.Status,Integer> getCounts() {
        Map<Job.Status,Integer> stats = new HashMap<>();
        synchronized (this) {
            for (Job job : jobQueue) {
                Job.Status status = job.getStatus();
                Integer i = stats.get(status);
                stats.put(status, i == null ? 1 : i + 1);
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
        log("Generating status report");
        StringBuffer sb = new StringBuffer();
        sb.append("Job manager: "+queueStatus+"\n");
        sb.append("Run depth..: "+runDepth+"\n");
        sb.append("JOB ID  STATUS           DESCRIPTION\n");
        sb.append("------  ---------------  ---------------------------------------\n");
        for (Job job : jobQueue)
            sb.append(String.format("%5d  %15s  %s\n",job.getId(),job.getStatus(),job.getDescription()));
        return sb.toString();
    }

    /**
     * Places the queue into the {@code SHUTTINGDOWN} state, after which no more jobs will be put into the
     * {@code RUNNING} state.  After all currently running jobs are stopped, the scheduler will perform a
     * {@code notifyWho.notifyAll()} to signal quiescence.
     * @param notifyWho The object which is waiting on a signal that the job queue is quiesced.
     */
    public void stopQueue(Object notifyWho) {
        log("Stop queue requested");
        synchronized (this) {
            queueStatus= QueueStatus.SHUTTINGDOWN;
            uponQuiesceNotify =notifyWho;
            notifyAll();
        }
    }

    /**
     * Places the queue into the {@code RUNNING} and forces a re-examination of its contents.
     */
    public void startQueue() {
        log("Start queue requested");
        synchronized (this) {
            queueStatus= QueueStatus.RUNNING;
            notifyAll();
        }

    }

    /** Returns the status of a job.
     *
     * @param jobId  The integer ID of the job to query.
     * @return NOSUCHJOB if the job does not exist, or the job's status.
     */
    synchronized public Job.Status getStatus(int jobId) {
        Job job = jobs.get(jobId);
        return job==null ? Job.Status.NOSUCHJOB : job.getStatus();
    }

    /**
     * Deletes a job from the queue if it is not in the {@code RUNNING} state.
     * @param jobId The ID of the job to delete.
     * @return True upon success, false if the job is in the {@code RUNNING} state.
     * @throws IllegalArgumentException if the job does not exist.
     */
    synchronized public boolean delete(int jobId) throws IllegalArgumentException {
        log("Request to delete job %d",jobId);
        Job job =jobs.get(jobId);
        if (job==null)
            throw new IllegalArgumentException(String.format("Job %d not found",jobId));
        if (job.getStatus().equals(Job.Status.RUNNING))
            return false;
        jobQueue.remove(job);
        jobs.remove(jobId);
        return true;
    }

    /** Removes all terminated entries from the queue.**/
    synchronized public void cleanup() {
        log("Cleanup requested");
        List<Job> targets = new LinkedList<>();
        for (Job job : jobQueue)
            if (job.getStatus().equals(Job.Status.TERMINATED))
                targets.add(job);
        for (Job job:targets) {
            jobQueue.remove(job);
            jobs.remove(job.getId());
            threads.remove(job);
        }
    }

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
    public Iterator<Job> iterator() {
        return new jobIterator();
    }

    /**
     * Looks up a job by matching the provided description.  The first job in the queue by job ID that matches the
     * description is returned, or null if one is not found.  Job description uniqueness is not enforced by the
     * Job Manager, so it is up to the user to provide uniqueness if it is important.
     * @param description
     * @return The first job in the queue that matches the description, or null.
     */
    public Job lookupByDescription(String description) {
        for (Job j : jobQueue)
            if (j.getDescription().equals(description))
                return j;
        return null;
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
                log("Scheduler waiting for event");
                try {
                    wait();
                } catch (InterruptedException e) {
                    log("The scheduler was awakened by an unexpected interrupt");
                    //Do nothing
                }
                log("Scheduler is scanning the queue");
                //System.out.println("--Scheduler tick--");
                //Run the queue here
                //First, a scan to see what's what
                List<Job> waitList = new LinkedList<>();
                int running=0;
                for (Job job : jobQueue) {
                    switch (job.getStatus()) {
                        case INTRODUCED:
                            log("Job %d INTRODUCED->WAITING",job.getId());
                            job.setStatus(Job.Status.WAITING);
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
                    log("Queue is now quiescent");
                    queueStatus= QueueStatus.QUIESCED;
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
                    log("Launching job %d",toRun.getId());
                    toRun.setStatus(Job.Status.LAUNCHING);
                    running++;
                    if (!kickOff(toRun)) {
                        running--;
                        toRun.setStatus(Job.Status.ERROR);
                        log("Job %d experienced an error at launch",toRun.getId());
                    } else
                        toRun.setStatus(Job.Status.RUNNING);
                    log("Job %d running",toRun.getId());
                }
            }
        }

    }

    /**
     * Creates a thread to run the job's {@code run()} code.  After completion, the job
     * will notify the scheduler that it has finished. (See {@link Job}.)
     * @param job The {@code Job} object to run.
     * @return True if the thread successfully launches.
     */
    private boolean kickOff(Job job) {
        log("Kicking off job %d",job.getId());
        try {
            Thread thread = new Thread(job);
            threads.put(job,thread);
            thread.start();
        } catch (Exception e) {
            log("An exception occurred while kicking off job %d - %s",job.getId(),e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * Defines an interface for functions to pass to the commandInterface to extend its
     * functionality for custom applications.
     */
      public interface CommandExtender {
        /** Executes the command and return true if recognized, or false if not recognized **/
        public boolean execute(String[] parameters);
        /** Returns a short list on a single line of extended commands **/
        public String help();
    }

    /**
     * Providers a command line interface for controlling the job manager.  An optional
     * command extender can be provided. If no-null, it is called to process commands
     * the main interface does not recognize.
     * @param extender  A command extension function, or null if there isn't one.
     */
    public  void commandInterface(CommandExtender extender) {
        String cmd="";
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print(">");
            System.out.flush();
            try {
                cmd = br.readLine();
            } catch (IOException e) {
                System.err.println("Exception: "+e.getMessage());
                break;
            }
            if (cmd==null)
                break;
            cmd=cmd.trim();
            String[] s= tokenize(cmd);
            if (s.length==0 || s[0].equals(""))
                continue;

            int numargs = s.length;
            Integer[] iargs = new Integer[numargs];
            for (int x=0; x<numargs; x++) {
                try {
                    iargs[x]=Integer.parseInt(s[x]);
                } catch (NumberFormatException e) {
                    iargs[x]=null;
                }
            }


            if (s[0].equals("exit") || s[0].equals("end")) {
                if (!getQueueStatus().equals(QueueStatus.QUIESCED)) {
                    System.out.println("You must stop the queue first using 'stop'");
                    continue;
                }
                if (getCounts().get(Job.Status.WAITING)!=null) {
                    System.out.println("There are waiting jobs in the queue. Use 'die' to force exit");
                    continue;
                }
                break;
            }

            if (s[0].equals("vdub")) {
                introduce("virtual dub", new BackgroundWorker()  {
                    @Override
                    public Object work() throws Exception {
                        final String vdubExecutable = "T:\\VPS\\VirtualDub\\vdub";
                        final String sylFile = "T:\\VPS\\VirtualDub\\go.syl";
                        Process process = new ProcessBuilder(vdubExecutable,"/i",sylFile,"T:\\vps\\test\\gexorge").start();
                        int rval = process.waitFor();
                        if (rval==0)
                            throw new Exception("VDUB FAILURE");

                        return "OK";
                    }
                });
                continue;
            }

            if (s[0].equals("die"))
                break;
            if (s[0].equals("cleanup")) {
                cleanup();
                continue;
            }
            if (s[0].equals("?")) {
                System.out.println("cleanup\ncounts\ndelete\ndie\nend\nexit\nlist\nset\nstart\nstop\n"+
                        ((extender==null)?"":extender.help()));
                continue;
            }
            if (s[0].equals("kill")) {
                if (iargs[1]==null)
                    System.out.println("Requires job ID");
                else {

                }
                continue;
            }
            if (s[0].equals("delete")) {
                if (iargs[1]==null) {
                    System.out.println("Requires a job ID");
                    continue;
                }
                try {
                    if (!delete(iargs[1]))
                        System.out.println("Job is running and cannot be deleted");
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                }
                continue;
            }
            if (s[0].equals("stop")) {
                final Boolean lynchpin=true;
                final Thread shutdownNotify = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Shutting down the job manager");
                        while (!getQueueStatus().equals(QueueStatus.QUIESCED))
                            synchronized (lynchpin) {
                                try {
                                    lynchpin.wait();
                                } catch (InterruptedException e) {}
                            }
                        System.out.println("Job queue quiesced");

                    }
                });
                stopQueue(lynchpin);
                shutdownNotify.start();
                continue;
            }
            if (s[0].equals("start")) {
                startQueue();
                continue;
            }
            if (s[0].equals("list")) {
                System.out.print(statusReport());
                continue;

            }
            if (s[0].equals("counts")) {
                Map<Job.Status,Integer> counts = getCounts();
                System.out.println("STATUS          COUNT");
                System.out.println("--------------- ---------");
                for (Job.Status status : counts.keySet()) {
                    System.out.println(String.format("%-15s %d",status.toString(),counts.get(status)));
                }
                continue;
            }
            if (s[0].equals("set")) {
                if (s.length==1) {
                    System.out.println("Set requires a target parameter");
                    continue;
                }
                if (s[1].equals("?")) {
                    System.out.println("set runDepth <int>");
                    continue;
                }
                if (s[1].equals("runDepth")) {
                    if (s.length<3) {
                        System.out.println("Requires an integer that is greater than 0");
                        continue;
                    }
                    Integer depth=0;
                    try {
                        setRunDepth(Integer.parseInt(s[2]));
                    } catch (IllegalArgumentException e) {
                        System.out.println("Requires an integer that is greater than 0");
                    }
                    continue;
                }
                if (extender==null || !extender.execute(s))
                    System.out.println("Unknown set target");
                continue;
            }

            if (s[0].equals("status")) {
                if (iargs[1]==null)
                    System.out.println("Job ID is required");
                else {
                    Job job = jobs.get(iargs[1]);
                    if (job==null)
                        System.out.println("No such job");
                    else {
                        System.out.println(job.status());
                    }
                }
                continue;
            }

            if (s[0].equals("add")) {
                if (s.length<2) {
                    System.out.println("Run duration in seconds is required");
                    continue;
                }
                int duration=0;
                try {
                    duration = Integer.parseInt(s[1]);
                } catch (NumberFormatException e){
                    System.out.println("Run duration must be an integer");
                    continue;
                }
                final long fduration=duration*1000;
                introduce(cmd, new BackgroundWorker() {
                    @Override
                    public Object work() {
                        System.out.println("Start sleeping for "+s[1]);
                        try {
                            sleep(fduration);
                        } catch (Exception e) {
                            System.out.println("Sleep interrupted");
                        }
                        System.out.println("Done sleeping for "+s[1]);
                        return null;
                    }
                });
                continue;
            }
            if (extender==null || !extender.execute(s))
                System.out.println("Unknown command");

        }
        System.exit(0);

    }

    /***
     * Parses a command line and returns an array of strings as tokens.
     * If a substring starts with a " or a ', then it is matched with the closing/matching " or '
     * and these quotes are removed.
     * @param s
     * @return
     */
    private static String[] tokenize(String s) {
        final int START = 0;
        final int TOKEN = 1;
        final int SINGLE = 2;
        final int DOUBLE =3;
        final int END = 5;
        int state = START;
        char[] ca = s.toCharArray();
        List<String> result = new LinkedList<String>();
        char c = 0;
        int idx = 0;
        int mark=-1;
        if (s==null)
            return new String[0];
        do {
            c= (idx>=ca.length) ? 0 : ca[idx];
            boolean isspace = Character.isSpaceChar(c);
            boolean isend = c==0;
            switch (state) {
                case START:
                    if (c=='"') {
                        mark = idx + 1;
                        state = DOUBLE;
                    } else if (c=='\'') {
                        mark=idx+1;
                        state = SINGLE;
                    } else if (isend)
                        state=END;
                    else if (!isspace){
                        mark=idx;
                        state=TOKEN;
                    }
                    break;
                case TOKEN:
                    if (isspace || isend) {
                        result.add(s.substring(mark,idx));
                        state=START;
                    }
                    break;
                case SINGLE:
                    if (c=='\'') {
                        result.add(s.substring(mark,idx));
                        state=START;
                    } else if (isend) {
                        return null;
                    }
                    break;
                case DOUBLE:
                    if (c=='"') {
                        result.add(s.substring(mark,idx));
                        state=START;
                    } else if (isend) {
                        return null;
                    }
                    break;
                case END:
                    break;
            }
            idx++;
        } while (c>0);
        return result.toArray(new String[0]);
    }

    public static void main(String[] args) {
        String cmd="";
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.print("parser test>");
            System.out.flush();
            try {
                cmd = br.readLine();
            } catch (IOException e) {
                System.err.println("Exception: " + e.getMessage());
                break;
            }
            String[] s = tokenize(cmd);
            if (s==null)
                System.out.print("Tokenization error");
            else if (s.length==0)
                System.out.print("No tokens returned");
            else
                for (int x=0; x<s.length; x++)
                    System.out.println(String.format("%d. '%s'",x,s[x]));
        }
    }

    private PrintStream logStream = System.err;

    public void SetLogStream(PrintStream ps) {
        logStream = ps;
    }

    private SimpleDateFormat logDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss ");

    public void log(String format, Object...args) {
        logStream.print(logDateFormat.format(new Date()));
        logStream.println(String.format(format,args));
    }


}
