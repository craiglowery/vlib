package com.craiglowery.java.vlib.clients.backup;

import com.craiglowery.java.common.Sha1FileTest;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.*;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class BackupDB implements AutoCloseable {

    private Connection connection=null;

    @Override
    public void close() throws Exception {
        if (connection!=null)
            connection.close();
    }

    public enum BackupStatusType  {
        /** A copy was initiated for this object but has not yet completed **/ copying,
        /** A file was copied but has not yet been verified **/ copied,
        /** The copy failed for some reason **/ copy_failed,
        /** A verification was initiated for this object but has not yet completed **/ verifying,
        /** The verification failed for some reason **/ verification_failed,
        /** A file has been copied and verified **/ verified,
        /** A verify was attempted but the required volume isn't mounted **/ volume_missing};

    public BackupDB(String url, String user, String password) throws SQLException {
        connection = DriverManager.getConnection(url,user,password);
    }

    /**
     * Registers a new backup volume in the databse.
     * @param backupvolumeuuid  The UUID of the volume.
     * @param backupvolumelabel  The volume label.
     * @param dateinitialized  The data initialized in SQL DATE format.
     * @throws Exception
     */
    public void registerNewBackupVolume(String backupvolumeuuid,
                                        String backupvolumelabel,
                                        String dateinitialized) throws Exception {
        try (Statement statement = connection.createStatement()) {
            String cmd =
                    String.format("INSERT INTO backupvolumes (backupvolumeuuid,backupvolumelabel,dateinitialized)" +
                                    " VALUES ('%s','%s','%s')",
                            backupvolumeuuid, backupvolumelabel, dateinitialized);
            int rows = statement.executeUpdate(cmd);
            if (rows != 1)
                throw new Exception(String.format("%d rows inserted: %s", rows, cmd));
        }
    }

    public class BackupVolume {
        String backupvolumeuuid;
        String backupvolumelabel;
        String dateinitialized_s;
        java.sql.Timestamp dateinitialized;
        File root;
        public BackupVolume(String uuid, String label, java.sql.Timestamp timestamp, File root) {
            backupvolumelabel=label;
            backupvolumeuuid=uuid;
            dateinitialized=timestamp;
            dateinitialized_s=timestamp.toString();
            this.root=root;
        }
        @Override
        public String toString() {
            return String.format("%s [%s] on %s: %s",backupvolumelabel,backupvolumeuuid,dateinitialized_s,
                    root==null?"mount point unknown":"mounted at "+root.getAbsolutePath());
        }
    }

    /**
     * Retrieves a backup volume descriptor by uuid.
     * @param uuid
     * @return
     * @throws SQLException
     */
    public BackupVolume getVolumeByUUID(String uuid) throws SQLException {
        try (Statement statement = connection.createStatement()){
            ResultSet rs = statement.executeQuery("SELECT * FROM backupvolumes WHERE backupvolumeuuid='"+uuid+"'");
            if (!rs.next())
                return null;
            return new BackupVolume(
                    rs.getString("backupvolumeuuid"),
                    rs.getString("backupvolumelabel"),
                    rs.getTimestamp("dateinitialized"),
                    null
            );
        }
    }

    /**
     * Retrieves a backup volume descriptor by volume label.
     * @param label
     * @return
     * @throws SQLException
     */
    public BackupVolume getVolumeByLabel(String label) throws SQLException {
        try (Statement statement = connection.createStatement()){
            ResultSet rs = statement.executeQuery("SELECT * FROM backupvolumes WHERE backupvolumelabel='"+label+"'");
            if (!rs.next())
                return null;
            return new BackupVolume(
                    rs.getString("backupvolumeuuid"),
                    rs.getString("backupvolumelabel"),
                    rs.getTimestamp("dateinitialized"),
                    null
            );
        }
    }

    public class BackupObject {
        public String sha1sum;
        public Long length;
        public String title;
        public Integer handle;
        public Integer versioncount;
        public String contenttype;
        public String blobkey;
        public Boolean iscurrent;
        public String imported;
        public java.sql.Timestamp timeimported;
        public java.sql.Timestamp timebackedup;
        public String backupvolumeuuid;
        public String backupstatus;
        public java.sql.Timestamp timeverified;

        public BackupObject(String sha1sum,
                            Long length,
                            String title,
                            Integer handle,
                            Integer versioncount,
                            String contenttype,
                            String blobkey,
                            Boolean iscurrent,
                            String imported,
                            Timestamp timeimported,
                            Timestamp timebackedup,
                            String backupvolumeuuid,
                            String backupstatus,
                            Timestamp timeverified) {
            this.sha1sum = sha1sum;
            this.length = length;
            this.title = title;
            this.handle = handle;
            this.versioncount = versioncount;
            this.contenttype = contenttype;
            this.blobkey = blobkey;
            this.iscurrent = iscurrent;
            this.imported=imported;
            this.timeimported = timeimported;
            this.timebackedup = timebackedup;
            this.backupvolumeuuid = backupvolumeuuid;
            this.backupstatus = backupstatus;
            this.timeverified = timeverified;
        }
        public BackupObject(ResultSet rs) throws Exception {
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                if (field.isSynthetic())
                    continue;
                switch (field.getType().getTypeName()) {
                    case "java.lang.String":
                        field.set(this,rs.getString(field.getName()));
                        break;
                    case "java.lang.Integer":
                        field.set(this,rs.getInt(field.getName()));
                        break;
                    case "java.lang.Boolean":
                        field.set(this,rs.getBoolean(field.getName()));
                        break;
                    case "java.sql.Timestamp":
                        field.set(this,rs.getTimestamp(field.getName()));
                        break;
                    case "java.lang.Long":
                        field.set(this,rs.getLong(field.getName()));
                        break;
                    default:
                        throw new Exception("Unexpected field type "+field.getType().getTypeName());
                }
            }
        }
    }

    private static int TIMEOUT_SECONDS = 30 * 60;  //30 minutes

    /**
     * Finds an object that has not already been selected for copy or failed previous copy, marks it as in progress,
     * and returns information about it.
     *
     * @return Null if there are no more objects to copy.
     * @throws Exception
     */
    public BackupObject selectObjectForCopy() throws Exception {
        try (Statement statement = connection.createStatement()) {
            boolean rollback=true;
            try {
                statement.execute("BEGIN WORK");
                String cmd = "SELECT * FROM versions WHERE iscurrent AND "+
                        "backupstatus=null OR backupstatus='' OR " +
                        "(backupstatus='%s' AND timebackedup<CURRENT_TIMESTAMP - INTERVAL '1 hour') " + //copying
                        "ORDER BY handle LIMIT 1 FOR UPDATE";
                cmd = String.format(cmd, BackupStatusType.copying);
                ResultSet rs = statement.executeQuery(cmd);
                if (!rs.next())
                    return null;
                BackupObject object = new BackupObject(rs);
                String snow = Timestamp.from(Instant.now()).toString();
                cmd = "UPDATE versions SET backupstatus='"+BackupStatusType.copying.toString()+"', timebackedup='%s' WHERE handle=%d AND imported='%s'";
                cmd=String.format(cmd,snow,object.handle,object.imported);
                int rows = statement.executeUpdate(cmd);
                if (rows!=1)
                    throw new Exception("Update failed for "+cmd);
                statement.execute("COMMIT");
                rollback=false;
                return object;
            } finally {
                if (rollback) statement.execute("ROLLBACK");
            }
        }

    }

    /**
     * Finds an object that has been copied but not verified, marks it as in progress,
     * and returns information about it.
     *
     * @return Null if there are no more objects to verify.
     * @throws Exception
     */
    public BackupObject selectObjectForVerification(boolean retry) throws Exception {
        try (Statement statement = connection.createStatement()) {
            boolean rollback=true;
            try {
                statement.execute("BEGIN WORK");
                String cmd = "SELECT * FROM versions WHERE iscurrent AND ("+
                        "backupstatus='%s' OR " +  //copied
                        " (%s) OR " +  //verification failed (optional)
                        "(backupstatus='%s' AND timeverified<CURRENT_TIMESTAMP - INTERVAL '30 minutes') " + //verifying
                        ") ORDER BY handle LIMIT 1 FOR UPDATE";
                String retryClause =
                        retry ?
                                String.format("backupstatus='%s'",BackupStatusType.verification_failed)
                              :
                                "FALSE";
                cmd = String.format(cmd, BackupStatusType.copied, retryClause, BackupStatusType.verifying);
                ResultSet rs = statement.executeQuery(cmd);
                if (!rs.next())
                    return null;
                BackupObject object = new BackupObject(rs);
                String snow = Timestamp.from(Instant.now()).toString();
                cmd = "UPDATE versions SET backupstatus='"+BackupStatusType.verifying.toString()+"', timeverified='%s' WHERE handle=%d AND imported='%s'";
                cmd=String.format(cmd,snow,object.handle,object.imported);
                int rows = statement.executeUpdate(cmd);
                if (rows!=1)
                    throw new Exception("Update failed for "+cmd);
                statement.execute("COMMIT");
                rollback=false;
                return object;
            } finally {
                if (rollback) statement.execute("ROLLBACK");
            }
        }

    }
    public void commitObjectBackupStatus(BackupObject object) throws Exception {
        try (Statement statement = connection.createStatement()) {
            boolean rollback=true;
            try {
                statement.execute("BEGIN WORK");
                String cmd = "SELECT * FROM versions WHERE handle=%d AND imported='%s' FOR UPDATE";
                cmd = String.format(cmd,object.handle,object.imported);
                ResultSet rs = statement.executeQuery(cmd);
                if (!rs.next())
                    throw new Exception(String.format("Object (%d,%s) %s not found",object.handle,object.imported,object.title));
                cmd = "UPDATE versions SET backupstatus='%s' WHERE handle=%d AND imported='%s'";
                cmd=String.format(cmd,object.backupstatus,object.handle,object.imported);
                int rows = statement.executeUpdate(cmd);
                if (rows!=1)
                    throw new Exception("Update failed for "+cmd);
                statement.execute("COMMIT");
                rollback=false;
            } finally {
                if (rollback)
                    statement.execute("ROLLBACK");
            }
        }
    }

    public void setBackupVolumeUUID(BackupObject object) throws Exception {
        setBackupVolumeUUID(object.handle,object.imported,object.backupvolumeuuid);
    }
    public void setBackupVolumeUUID(int handle, String imported, String backupvolumeuuid) throws Exception {
        try (Statement statement = connection.createStatement()) {
            boolean rollback=true;
            try {
                statement.execute("BEGIN WORK");
                String cmd = "SELECT * FROM versions WHERE handle=%d AND imported='%s' FOR UPDATE";
                cmd = String.format(cmd,handle,imported);
                ResultSet rs = statement.executeQuery(cmd);
                if (!rs.next())
                    throw new Exception(String.format("Object (%d,%s) not found",handle,imported));
                cmd = "UPDATE versions SET backupvolumeuuid='%s' WHERE handle=%d AND imported='%s'";
                cmd=String.format(cmd,backupvolumeuuid,handle,imported);
                int rows = statement.executeUpdate(cmd);
                if (rows!=1)
                    throw new Exception("Update failed for "+cmd);
                statement.execute("COMMIT");
                rollback=false;
            } finally {
                if (rollback)
                    statement.execute("ROLLBACK");
            }
        }
    }


    public static void forceVolumeuuid() {
        try (
            BackupDB bdb = new BackupDB("jdbc:postgresql://192.168.1.14/vlibrary", "vbackup", "DrT4U&pv1865");
        ) {
            Map<String,File> volMap = new HashMap<>();
            volMap.put("VLIBOBJ_1",new File("J:\\"));
            volMap.put("VLIBOBJ_2",new File("O:\\"));
            try (Statement statement = bdb.connection.createStatement()) {
                for (String volumeLabel : volMap.keySet()) {
                    BackupVolume backupVolume = bdb.getVolumeByLabel(volumeLabel);
                    if (backupVolume == null)
                        throw new Exception("Backup volume is null for " + volumeLabel);
                    Files.walkFileTree(
                            volMap.get(volumeLabel).toPath(),
                            new FileVisitor<Path>() {
                                @Override
                                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                                    return FileVisitResult.CONTINUE;
                                }

                                @Override
                                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                                    if (file.toString().endsWith(".meta")) {
                                        String blobkey = file.toAbsolutePath().getFileName().toString().substring(0, 36);
                                        try {
                                            statement.executeUpdate(String.format("UPDATE versions SET backupvolumeuuid='%s' WHERE blobkey='%s'",
                                                    backupVolume.backupvolumeuuid, blobkey));
                                        } catch (Exception e) {
                                            throw new Error(e);
                                        }
                                        System.out.println(blobkey);
                                    }
                                    return FileVisitResult.CONTINUE;
                                }

                                @Override
                                public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                                    return FileVisitResult.CONTINUE;
                                }

                                @Override
                                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                                    return FileVisitResult.CONTINUE;
                                }
                            }
                    );
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    PreparedStatement logPreparedStatement = null;
    public void log(int handle, String blobkey, String backupvolumeuuid, String backupStatus, String info)  {
        String textEntry =
                String.format("LOG: %s handle=%d blobkey=%s backupvolumeuuid=%s backupstatus=%s info=%s",
                        LocalDateTime.now(Clock.systemDefaultZone()).toString().substring(0,21),
                        handle,blobkey,backupvolumeuuid,backupStatus,info);
        try {
            if (logPreparedStatement == null)
                logPreparedStatement = connection.prepareStatement("INSERT INTO backuplog (eventstamp,handle,blobkey,backupvolumeuuid,backupstatus,info) " +
                        "VALUES (current_timestamp,?,?,?,?,?)");
            //                                                                                            1      2       3                4            5
            logPreparedStatement.setInt(1, handle);
            logPreparedStatement.setString(2, blobkey);
            logPreparedStatement.setString(3, backupvolumeuuid);
            logPreparedStatement.setString(4, backupStatus);
            logPreparedStatement.setString(5, info);
            logPreparedStatement.execute();
            System.out.println(textEntry);
        } catch (Exception e) {
            System.err.println(String.format("Logging failed: %s: %s", e.getClass().getTypeName(),e.getMessage()));
            System.err.println(textEntry);
        }
    }

}
