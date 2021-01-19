package com.craiglowery.java.common;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;


public class DeleteEmptyDirs {

    /**
     * Looks at directories inside of the named directory and removes any that are empty.
     * It is not recursive.
     * @param pathString
     */
    public static void deleteEmptyDirectories(String pathString) throws Exception {
        Path dirPath = new File(pathString).toPath();
        Files.list(dirPath).forEach(
                (path) ->  {
                    if (Files.isDirectory(path)) {
                        try {
                            if (Files.list(path).count() == 0) {
                                try {
                                    Files.delete(path);
                                    System.out.println("DELETED: "+path.toString());
                                } catch (Exception e) {
                                    throw new Error(String.format("Could not delete %s: %s: %s",path.toAbsolutePath(),e.getClass().getName(),e.getMessage()),e);
                                }
                            }
                        } catch (Exception e) {
                            throw new Error("Error listing subdir "+dirPath.toString()+" :"+e.getMessage());
                        }

                    }
                }
        );
    }

    public static void renameMkvs(String pathString) throws Exception {
        Path dirPath = new File(pathString).toPath();
        Files.list(dirPath).forEach(
                (path) ->  {
                    if (Files.isDirectory(path)) {
                        try {
                            if (Files.list(path).count() == 1) {
                                //Is it an MKV?
                                Optional<Path> candidate = Files.list(path).findFirst();
                                if (candidate.isEmpty())
                                    throw new Error("There should be at least one path - programming error");
                                Path subPath = candidate.get();
                                String suffix = Util.endsWithKnownVideoExtension(subPath.getFileName().toString());
                                if (suffix==null)
                                    return;
                                //Rename it to the parent directory name
                                String newFilename = path.getFileName() + suffix;
                                //Move it to "ready" directory
                                Files.createDirectories(dirPath.resolve(("ready")));
                                Path newPath = dirPath.resolve("ready").resolve(newFilename);
                                Files.move(subPath,newPath);
                                Files.delete(path);
                            }
                        } catch (Exception e) {
                            throw new Error("Error listing subdir "+dirPath.toString()+" :"+e.getMessage());
                        }

                    }
                }
        );

    }

    public static void main(String ... args) {
        try {
            //deleteEmptyDirectories("\\\\VENC\\Users\\Media\\Downloads\\complete");
            renameMkvs("\\\\VENC\\Users\\Media\\Downloads\\complete");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
