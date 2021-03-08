package com.craiglowery.java.vlib.clients.backup;


import com.craiglowery.java.common.FinalBoolean;
import com.craiglowery.java.common.ObjectMetaWrapper;
import com.craiglowery.java.common.Sha1FileTest;
import com.craiglowery.java.common.Util;

import javax.swing.filechooser.FileSystemView;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maintains backups of the blob store by copying blob and metafiles to removable disks.
 * These disks are identified by scanning for volume labels with a specific prefix
 * (by default, the prefix is VLIBOBJ_).  The structure of a backup disks is:
 *
 *    /blob
 *    /db
 *
 *    Where blob is the directory structure containing the blob files, an db is a subdirectory
 *    containing JSON files that represent the entire VLIB database of video objects.
 *    The JSON files have names of the for
 *
 *               vlibobj_yyyymmdd_hhmmss.json
 *
 */
public class VlibBackup {

    public String BACKUP_VOLUME_PREFIX = "VLIBOBJ_";
    public static String UUID_FILE = "vlibobj.backup.uuid";

    private static long SLEEP_INTERVAL = 10000;

    public static Path pathToServerBlobStore = new File("\\\\jclwhs\\videos\\lib\\blob").toPath();

    private Map<String, BackupDB.BackupVolume> backupVolumesByVolumeLabel;
    private Map<String, BackupDB.BackupVolume> backupVolumesByVolumeUUID;

    private int MAX_META_LENGTH=1024*1024;   //Maximum length of JSON file (meta data) in blob store - same as in VLIB server LinuxFSBlobStore class

    Random random = new Random(Instant.now().toEpochMilli());

    public String url;
    public String user;
    public String password;

    public VlibBackup(String url, String user, String password) throws Exception {
        //Cache database credentials
        this.url=url;
        this.user=user;
        this.password=password;

        //Find and index backup volumes by volume label and uuid
        backupVolumesByVolumeLabel = getBackupVolumes(true);
        backupVolumesByVolumeUUID = new HashMap<>();
        for (BackupDB.BackupVolume backupVolume : backupVolumesByVolumeLabel.values())
            backupVolumesByVolumeUUID.put(backupVolume.backupvolumeuuid,backupVolume);
        if (backupVolumesByVolumeLabel.size() == 0)
            log("No backup volumes found");
        else
            for (String volumeName : backupVolumesByVolumeLabel.keySet())
               log("%s (%s)", volumeName, backupVolumesByVolumeLabel.get(volumeName).root.getAbsolutePath());
    }

    private String chooseRandomVolume() {
        return backupVolumesByVolumeLabel.keySet().toArray(new String[0])[random.nextInt(backupVolumesByVolumeLabel.size())];
    }

    private int roundRobinPointer=0;
    synchronized private String chooseRoundRobinVolume() {
        roundRobinPointer = (roundRobinPointer+1) % backupVolumesByVolumeLabel.size();
        //System.out.println("RR="+roundRobinPointer);
        return backupVolumesByVolumeLabel.keySet().toArray(new String[0])[roundRobinPointer];
    }

    private Supplier<String> volumeChooser = this::chooseRoundRobinVolume;

    /**
     * <p>Scans the Windows system for Local Disks that are valid backup destinations.  A valid
     * backup destination must meet these requirements:</p>
     *
     * <ul>
     *     <li>Be labeled with a string that has the prefix {@code BACKUP_VOLUME_PREFIX}</li>
     *     <li>Has only two subdirectories in the root: /blob and /db</li>
     *     <li>Has a singular file in the root directory {@code UUID_FILE} which contains the 36 character
     *     UUID unique to that volume.</li>
     * </ul>
     *
     * @param autoInitialize If{@code true} then disks that are labeled with the {@code BACKUP_VOLUME_PREFIX}
     *                       but have an empty file system will be initialized as an empty disk, by creating
     *                       the two required directories, assigning a UUID and placing it in the
     *                       UUID_FILE file of the root.
     * @return Map of local disks by label, if the label has the BACKUP_VOLUME_PREFIX.
     * @throws Exception if a volume is labeled as a backup volume but does not  or cannot meet
     * the other two requirements.
     */
    public Map<String, BackupDB.BackupVolume> getBackupVolumes(boolean autoInitialize) throws Exception {
        File[] roots = File.listRoots();
        Map<String, BackupDB.BackupVolume> result = new HashMap<>();
        File[] drives = File.listRoots();
        try (BackupDB bdb = new BackupDB(url,user,password)) {
            if (drives != null && drives.length > 0) {
                for (File aDrive : drives) {
                    try {
                        if (fileSystemView.getSystemTypeDescription(aDrive).equals("Local Disk")) {
                            String volumeLabel = getVolumeLabel(aDrive);
                            if (volumeLabel != null && volumeLabel.startsWith(BACKUP_VOLUME_PREFIX)) {
                                //The disk is properly labeled, but does it have the right structure?
                                //List all files
                                Map<String, File> files = mapFiles(aDrive);
                                if (files.size() == 0) {
                                    if (autoInitialize) {
                                        initializeVolume(aDrive);
                                        files = mapFiles(aDrive);
                                    } else
                                        throw new Exception("Uninitialized volume (autoInitialize is false)");
                                }
                                //Is the label one we've registered in the past?
                                BackupDB.BackupVolume backupVolume = bdb.getVolumeByLabel(volumeLabel);
                                if (backupVolume == null)
                                    throw new Exception("volume " + volumeLabel + " is not registered");
                                backupVolume.root=aDrive;
                                //Is there a "blob" directory?
                                if (!files.containsKey("blob") || !files.get("blob").isDirectory())
                                    throw new Exception("no 'blob' directory");
                                files.remove("blob");
                                //Is there a "db" directory?
                                if (!files.containsKey("db") || !files.get("db").isDirectory())
                                    throw new Exception("no 'db' directory");
                                files.remove("db");
                                //Is there a UUID_FILE file?
                                if (!files.containsKey(UUID_FILE) || !files.get(UUID_FILE).isFile())
                                    throw new Exception("no '" + UUID_FILE + "' file");
                                //Is this a registered UUID?
                                String uuid = readUUID(files.get(UUID_FILE));
                                if (uuid == null) {
                                    throw new Exception("malformed UUID");
                                }
                                BackupDB.BackupVolume bv = bdb.getVolumeByUUID(uuid);
                                if (bv == null)
                                    throw new Exception(String.format("UUID %s is not found in the registry", uuid));
                                if (!bv.backupvolumelabel.equals(volumeLabel))
                                    throw new Exception(String.format("UUID %s was registered with a different volume label (%s) not with %s",
                                            uuid, bv.backupvolumelabel, volumeLabel));
                                files.remove(UUID_FILE);
                                //Is there anything else?
                                if (files.size() != 0)
                                    throw new Exception("Extraneous directory entries in root: " + String.join(", ", files.keySet()));
                                result.put(volumeLabel,backupVolume);
                            }
                        }
                    } catch (Exception e) {
                        throw new Exception(String.format("DRIVE %s - %s", aDrive.getAbsolutePath(), e.getMessage()));
                    }
                }
            }
        }
        return result;
    }

    private static Pattern patternVolumeLabel = Pattern.compile("^(.+) \\([A-Z]\\:\\)$");
    private static FileSystemView fileSystemView = FileSystemView.getFileSystemView();
    public static String getVolumeLabel(File file) {
        Matcher matcher = patternVolumeLabel.matcher(fileSystemView.getSystemDisplayName(file));
        if (matcher.matches())
            return matcher.group(1);
        return null;
    }

    /**
     * Gets a map of file names to File objects. Ignores special entries like "System Volume Information"
     * @param dir
     * @return
     */
    private static Map<String,File> mapFiles(File dir) {
        Map<String,File> result = new HashMap<>();
        for (File file : dir.listFiles()) {
            String name = file.getName();
            if (name.toUpperCase().equals("SYSTEM VOLUME INFORMATION"))
                continue;
            if (name.toUpperCase().equals("$RECYCLE.BIN"))
                continue;
            result.put(name,file);
        }
        return result;
    }

    private Pattern UUIDPattern = Pattern.compile("^[0-9a-f]{8,8}-([0-9a-f]{4,4}-){3,3}[0-9a-f]{12,12}$");
    /**
     * Attempts to read the UUID from the special file in the root directory.
     * @return The UUID read, or null if there was no uuid file found.
     */
    private String readUUID(File uuidFile) throws Exception {
        if (uuidFile.length()!=37)
            throw new Exception(String.format("File %s doesn't seem to be a UUID file (wrong length)",uuidFile.getAbsolutePath()));
        try (FileInputStream fis = new FileInputStream(uuidFile)) {
            String uuidString = new String(fis.readNBytes(36));
            if (!UUIDPattern.matcher(uuidString).matches())
                throw new Exception(String.format("File %s doesn't seem to be a UUID file (unexpected form)",uuidFile.getAbsolutePath()));
            return uuidString;
        }
    }

    /**
     * Intializes a new backup volume.  The root directory must be empty.
     * @param root The root directory to initialize.
     * @throws Exception
     */
    public void initializeVolume(File root) throws Exception {
        log("Initializing %s",fileSystemView.getSystemDisplayName(root));
        try (BackupDB bdb = new BackupDB(url,user,password)){
            String volumeLabel = getVolumeLabel(root);
            if (bdb.getVolumeByLabel(volumeLabel)!=null)
                throw new Exception("This volume was previously initialized and registered. Can't re-initialize");
            if (mapFiles(root).size()!=0)
                throw new Exception("Root directory is not empty");
            Path rootPath = root.toPath();
            rootPath.resolve("blob").toFile().mkdir();
            rootPath.resolve("db").toFile().mkdir();
            File uuid_file = rootPath.resolve(UUID_FILE).toFile();
            UUID uuid = java.util.UUID.randomUUID();
            try (FileOutputStream fos = new FileOutputStream(uuid_file)) {
                fos.write(uuid.toString().getBytes());
                fos.write('\n');
            }
            uuid_file.setReadOnly();
            bdb.registerNewBackupVolume(uuid.toString(),volumeLabel,java.sql.Timestamp.from(Instant.now()).toString());

        } catch (Exception e) {
            throw new Exception(String.format("While initializing %s: %s",
                    root.getAbsolutePath(),e.getMessage()));
        }
    }



    private static int COPY_BUFFER_SIZE = 1024*1024*8;
    private static void copyFile(Path from, Path to, Consumer<String> statusUpdate) throws Exception {
        byte[] buf = new byte[COPY_BUFFER_SIZE];
        try (FileInputStream in   = new FileInputStream(from.toFile());
             FileOutputStream out = new FileOutputStream(to.toFile(),false)) {
            int pctComplete=0;
            int lastUpdatedPctComplete=0;
            long length = from.toFile().length();
            String displayLength = ""+(length/(1024*1024))+"M";
            statusUpdate.accept("0% of "+displayLength);
            long totalRead=0;
            int bytesRead = in.read(buf);
            while (bytesRead>=0) {
                totalRead += bytesRead;
                pctComplete = (int)((((double)totalRead)/length)*100);
                if (pctComplete>=lastUpdatedPctComplete+5) {
                    lastUpdatedPctComplete=pctComplete;
                    statusUpdate.accept(String.format("%d%% of %s",pctComplete,displayLength));
                }
                out.write(buf,0,bytesRead);
                bytesRead = in.read(buf);
                Thread.yield();
            }
        }
    }

    private void copyAnObject(boolean threadAware) throws Exception {
        int jobNumber=getNextJobNumber();
        try (BackupDB bdb = new BackupDB(url,user,password)) {
            BackupDB.BackupObject object = bdb.selectObjectForCopy();
            if (object==null)
                throw new Exception(NO_OBJECTS_FOR_COPY);
            String threadName = String.format("Copy %d: %s (%d,%s) {%s}",
                    jobNumber, object.title,object.handle,object.imported,object.blobkey);
            if (threadAware)
                Thread.currentThread().setName(threadName+" [init]");
            String volume=null;
            if (object.backupvolumeuuid==null || object.backupvolumeuuid.equals("")) {
                log("INFORMATION: %s was previously backed up to (%s,%s) -> overwritting",object.handle,object.backupvolumeuuid,volume);
                volume=backupVolumesByVolumeUUID.get(object.backupvolumeuuid).backupvolumelabel;
            }
            if (volume==null)
                volume = volumeChooser.get();
            object.backupvolumeuuid= backupVolumesByVolumeLabel.get(volume).backupvolumeuuid;
            if (object.backupvolumeuuid==null)
                log("WARNING: Missing UUID for volume '%s'",volume);
            bdb.setBackupVolumeUUID(object);
            log("Copying '%s' {%s} (%d,%s) --> %s", object.title, object.blobkey, object.handle, object.imported,volume);
            Path copyToDirectory = Util.blobDirectory(backupVolumesByVolumeLabel.get(volume).root.toPath().resolve("blob"), object.blobkey);
            Path copyFromDirectory = Util.blobDirectory(pathToServerBlobStore, object.blobkey);

            Path pathFromBlob = copyFromDirectory.resolve(object.blobkey + ".blob");
            Path pathPendingBlob = copyToDirectory.resolve(object.blobkey + ".blob.pending");
            Path pathCommittedBlob = copyToDirectory.resolve(object.blobkey + ".blob");

            Path pathFromMeta = copyFromDirectory.resolve(object.blobkey + ".meta");
            Path pathPendingMeta = copyToDirectory.resolve(object.blobkey + ".meta.pending");
            Path pathCommittedMeta = copyToDirectory.resolve(object.blobkey + ".meta");

            try {
                //Create destination directories if needed
                Files.createDirectories(copyToDirectory);

                //Copy files to pending
                //System.out.println(String.format("   %s -> %s", pathFromBlob.toAbsolutePath(), pathToBlob.toAbsolutePath()));
                //Files.copy(pathFromBlob, pathToBlob);
                Instant startCopy = Instant.now();
                copyFile(pathFromBlob, pathPendingBlob, (pct) -> {
                    if (threadAware)
                        Thread.currentThread().setName(threadName + " [blob " + pct + "]");
                });
                Instant endCopy = Instant.now();
                long seconds = endCopy.getEpochSecond() - startCopy.getEpochSecond();
                long bytesPerSecond = pathFromBlob.toFile().length() / seconds;

                //System.out.println(String.format("   %s -> %s", pathFromMeta.toAbsolutePath(), pathToMeta.toAbsolutePath()));
                //Files.copy(pathFromMeta, pathToMeta);
                copyFile(pathFromMeta, pathPendingMeta, (pct) -> {
                    if (threadAware)
                        Thread.currentThread().setName(threadName + " [meta " + pct + "]");
                });

                bdb.log(object.handle, object.blobkey, object.backupvolumeuuid, BackupDB.BackupStatusType.copied.toString(), String.format("%dkb/s", bytesPerSecond / 1024));
                //Do a sha hashcode check with the database

                //Commit files
                Files.deleteIfExists(pathCommittedBlob);  //In case there is already one from a previous copy.
                Files.deleteIfExists(pathCommittedMeta);
                pathPendingMeta.toFile().renameTo(pathCommittedMeta.toFile());
                pathPendingBlob.toFile().renameTo(pathCommittedBlob.toFile());

                if (threadAware)
                    Thread.currentThread().setName(threadName + " [complete]");
                //Update database
                object.backupstatus = BackupDB.BackupStatusType.copied.toString();
                bdb.commitObjectBackupStatus(object);
            } finally {
                //Delete pending files if we failed for some reason
                Files.deleteIfExists(pathPendingBlob);
                Files.deleteIfExists(pathPendingMeta);
            }
        }
    }

    private void verifyAnObject(boolean threadAware, boolean retry) throws Exception {
        int jobNumber=getNextJobNumber();
        try (BackupDB bdb = new BackupDB(url,user,password)) {
            BackupDB.BackupObject object = bdb.selectObjectForVerification(retry);
            if (object==null)
                throw new Exception(NO_OBJECTS_FOR_VERIFY);
            String threadName = String.format("Verify %d: %s (%d,%s) {%s}",
                    jobNumber, object.title,object.handle,object.imported,object.blobkey);
            if (threadAware)
                Thread.currentThread().setName(threadName+" [init]");
            BackupDB.BackupVolume backupVolume = backupVolumesByVolumeUUID.get(object.backupvolumeuuid);
            if (backupVolume==null) {
                object.backupstatus= BackupDB.BackupStatusType.verification_failed.toString();
                bdb.commitObjectBackupStatus(object);
                String info = String.format("Cant' verify '%s' {%s} (%d,%s) - volumeuuid %s is not mounted",
                        object.title,object.blobkey,object.handle,object.imported,object.backupvolumeuuid);
                bdb.log(object.handle,object.blobkey,object.backupvolumeuuid,object.backupstatus, info );
                throw new Exception(info);
            }

            File root = backupVolume.root;

            log("Verifying '%s' {%s} (%d,%s) <-- %s", object.title, object.blobkey, object.handle, object.imported,root.getAbsolutePath());
            Path verifyDirectory = Util.blobDirectory(root.toPath().resolve("blob"), object.blobkey);
            Path pathVerifyBlob = verifyDirectory.resolve(object.blobkey + ".blob");
            Path pathVerifyMeta = verifyDirectory.resolve(object.blobkey + ".meta");

            //Read the meta file
            ObjectMetaWrapper objectMetaWrapper = null;
            try (FileReader fileReader = new FileReader(pathVerifyMeta.toFile())) {
                char[] buf = new char[5096];
                int length = fileReader.read(buf);
                if (length >MAX_META_LENGTH)
                    throw new Exception("Meta file exceeds maximum expected length");
                objectMetaWrapper= new ObjectMetaWrapper(new String(buf,0,length));
            } catch (Exception e) {
                object.backupstatus= BackupDB.BackupStatusType.verification_failed.toString();
                bdb.commitObjectBackupStatus(object);
                String info = String.format("Cant' verify '%s' {%s} (%d,%s) - error reading metafile: %s: %s",
                        object.title,object.blobkey,object.handle,object.imported,e.getClass().getName(),e.getMessage());
                bdb.log(object.handle,object.blobkey,object.backupvolumeuuid,object.backupstatus,info);
                throw new Exception(info);

            }
            //Compare sha1 in meta with sha1 in database (object)
            if (!objectMetaWrapper.getSha1sum().equals(object.sha1sum)) {
                object.backupstatus= BackupDB.BackupStatusType.verification_failed.toString();
                bdb.commitObjectBackupStatus(object);
                String info = String.format("Cant' verify '%s' {%s} (%d,%s) - sha1sum in db and meta file don't agree",
                        object.title,object.blobkey,object.handle,object.imported,root.getAbsolutePath());
                bdb.log(object.handle,object.blobkey,object.backupvolumeuuid,object.backupstatus,info);
                throw new Exception(info);
            }
            //Compute sha1 for copied object
            Consumer<Integer> statusUpdate = (percent) -> {
                Thread.currentThread().setName(threadName+" ["+percent+"%]");
            };
            if (!Sha1FileTest.verifyChecksum(pathVerifyBlob.toFile().getAbsolutePath(),object.sha1sum,statusUpdate)) {
                object.backupstatus= BackupDB.BackupStatusType.verification_failed.toString();
                bdb.commitObjectBackupStatus(object);
                String info = String.format("Verification failed '%s' {%s} (%d,%s) - computed sha1sum from copied file is incorrect",
                        object.title,object.blobkey,object.handle,object.imported);
                bdb.log(object.handle,object.blobkey,object.backupvolumeuuid,object.backupstatus,info);
                throw new Exception(info);
            }

            //Verification has passed


            bdb.log(object.handle,object.blobkey,object.backupvolumeuuid, BackupDB.BackupStatusType.verified.toString(),"");

            if (threadAware)
                Thread.currentThread().setName(threadName+" [complete]");
            //Update database
            object.backupstatus= BackupDB.BackupStatusType.verified.toString();
            bdb.commitObjectBackupStatus(object);
        }
    }





    private Integer jobCounter=0;
    private int getNextJobNumber() {
        synchronized (jobCounter) {
            return jobCounter++;
        }
    }

    private static boolean stopBackgroundWork=false;
    private void concurrentWorker(Runnable worker, Semaphore semaphore, FinalBoolean goToSleep) {
        final String threadName = Thread.currentThread().getName();
        int subThreadCount=0;
        log("%s starting",threadName);

        Runnable wrapper = new Runnable() {
            @Override
            public void run() {
                try { Thread.sleep(250); } catch (Exception e){}
                worker.run();
            }
        };

        try {
            do {
                do { //Keep looking for work until there is no more work or asked to stop
                    semaphore.acquire();
                    if (stopBackgroundWork || goToSleep.get()) {
                        semaphore.release();
                        break;
                    }
                    subThreadCount++;
                    final Thread thread = new Thread(wrapper);
                    log("%s starting subthread %d",threadName,subThreadCount );
                    thread.start();
                    log("%s subthread %d ended",threadName,subThreadCount);
                    Thread.sleep(1000);   //We always pause between loops
                } while (!(goToSleep.get() || stopBackgroundWork));

                if (goToSleep.get() && !stopBackgroundWork) {
                    //Sleep for a period and try again, or until we receive a wakeup
                    try {
                        log("%s: going to sleep",threadName);
                        Thread.sleep(SLEEP_INTERVAL);
                        log("%s: waking up",threadName);
                        goToSleep.set(false);
                    } catch (InterruptedException e) {
                        log("%s: sleep interrupted - waking up",threadName);
                        //Do nothing. We're just going to wake up early.
                    }
                }

            } while (!stopBackgroundWork);
        } catch (InterruptedException e) {
            log("%s: interrupted while waiting on semaphore acuisition",threadName);
        }
        log("%s exiting",threadName);

    }

    private static final String NO_OBJECTS_FOR_COPY = "No objects found for copy";
    private static final String NO_OBJECTS_FOR_VERIFY = "No objects found for verify";

    private void concurrentCopy(int n) {

        final Semaphore concurrentCopySemaphore = new Semaphore(n);
        final FinalBoolean copyGoToSleep = new FinalBoolean(false);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    copyAnObject(true);
                } catch (Exception e) {
                    if (e.getMessage().equals(NO_OBJECTS_FOR_COPY)) {
                        log(NO_OBJECTS_FOR_COPY);
                        copyGoToSleep.set(true);
                        return;
                    }
                    log("Copy of an object failed: %s: %s",e.getClass().getName(),e.getMessage());
                } finally {
                    concurrentCopySemaphore.release();
                }
            }
        };

        concurrentWorker(runnable, concurrentCopySemaphore, copyGoToSleep);
    }


    private void concurrentVerify(int n) {

        Semaphore concurrentVerifySemaphore = new Semaphore(n);
        final FinalBoolean verifyGoToSleep = new FinalBoolean(false);

        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    verifyAnObject(true, false);
                } catch (Exception e) {
                    if (e.getMessage().equals(NO_OBJECTS_FOR_VERIFY)) {
                        log(NO_OBJECTS_FOR_VERIFY);
                        verifyGoToSleep.set(true);
                        return;
                    }
                    log("Verification of an object failed: %s: %s", e.getClass().getName(), e.getMessage());
                } finally {
                    concurrentVerifySemaphore.release();
                }
            }
        };
        concurrentWorker(runnable, concurrentVerifySemaphore, verifyGoToSleep);
    }


    interface Procedure {
        public void execute();
    }

    static class JobDescriptor {
        String name;
        String title;
        String handle;
        String imported;
        String blobkey;
        String status;

        public JobDescriptor(String name, String title, String handle, String imported, String blobkey, String status) {
            this.name = name;
            this.title = title;
            this.handle = handle;
            this.imported = imported;
            this.blobkey = blobkey;
            this.status = status;
        }
    }

    static Map<String,JobDescriptor> activeJobs = new TreeMap<>();

    static Pattern jobName = Pattern.compile("^((?:Copy|Verify) \\d+): (.+) \\((\\d+),(\\d+)\\) \\{(.+)\\} \\[(.+)\\]$");
    private static void updateActiveJobs() {
        Thread[] threads = new Thread[Thread.activeCount()+20];
        int numThreads = Thread.enumerate(threads);
        synchronized (activeJobs) {
            activeJobs.clear();
            for (int t=0; t<numThreads; t++) {
                String name = threads[t].getName();
                if (!name.startsWith ("Copy ") && !name.startsWith("Verify "))
                    continue;
                Matcher matcher = jobName.matcher(name);
                if (!matcher.matches())
                    continue;
                String jobName=matcher.group(1);
                activeJobs.put(jobName,
                        new JobDescriptor(
                                jobName,
                                /* title */ matcher.group(2),
                                /* handle */ matcher.group(3),
                                /* imported */ matcher.group(4),
                                /* blobkey */ matcher.group(5),
                                /* status */ matcher.group(6)
                        )
                );
            }
        }
    }

    public static String getStatusReport() {
        StringBuffer buf = new StringBuffer();

        buf.append("STATUS               JOB NAME     TITLE                     HANDLE IMPORTED       BLOBKEY\n");
        buf.append("==================== ============ ========================= ====== ============== ====================================\n");
                //  20                     12           25                        6      14           36
        synchronized (activeJobs) {
            for (JobDescriptor jd : activeJobs.values()) {
                buf.append(String.format("%-20s %-12s %-25.25s %-6s %-14s %-36s\n",
                        jd.status,jd.name,jd.title,jd.handle,jd.imported,jd.blobkey));
            }
        }
        return buf.toString();
    }

    public static void main(String ... args) {
        try {
            Thread copyThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new VlibBackup("jdbc:postgresql://192.168.1.14/vlibrary", "vbackup", "DrT4U&pv1865").concurrentCopy(4);
                    } catch (Exception e) {
                        log("Concurrent copy threw an exception");
                        e.printStackTrace();
                    }
                }
            });
            Thread verifyThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        new VlibBackup("jdbc:postgresql://192.168.1.14/vlibrary", "vbackup", "DrT4U&pv1865").concurrentVerify(2);
                    } catch (Exception e) {
                        log("Concurrent verify threw an exception");
                        e.printStackTrace();
                    }
                }
            });
            copyThread.setName("Backup Copier");
            copyThread.start();
            verifyThread.setName("Backup Verifier");
            verifyThread.start();

            Thread statusUpdater = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            Thread.sleep(3000);
                            updateActiveJobs();
                        } catch (Exception e){}
                    }
                }
            });

            statusUpdater.start();

            while (true ) {
                System.out.print(">");
                System.out.flush();
                char c = (char)System.in.read();
                switch (c) {
                    case 's': System.out.println(getStatusReport());
                        break;
                    case 't': stopBackgroundWork = !stopBackgroundWork;
                        System.out.println("Stop background work now = "+stopBackgroundWork);
                        break;
                    case '\n':
                        break;
                    default:
                        System.out.println("?");
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static PrintStream logStream = null;
    private static DateTimeFormatter logTimeStampFormatter;
    static {
        try {
            logStream = new PrintStream(new FileOutputStream(new File("T:\\temp\\objectbackup.log"),true));
            logTimeStampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd hh:mm:ss a");
        } catch (Exception e) {
            throw new Error("Could not establish log stream: "+e.getClass().getName()+" :"+e.getMessage(), e);
        }
    }

    public static void log(String fmt, Object ... args) {
        LocalDateTime now = LocalDateTime.now();
        String msg = String.format("%s  %s\n",
            logTimeStampFormatter.format(now),
            String.format(fmt,args)
        );
        System.out.print(msg);
        System.out.flush();
        logStream.print(msg);
    }

}
