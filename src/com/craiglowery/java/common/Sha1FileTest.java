package com.craiglowery.java.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Function;


public class Sha1FileTest {

    /**
     * Verifies file's SHA1 checksum
     * @param file and name of a file that is to be verified
     * @param testChecksum the expected checksum
     * @return true if the expeceted SHA1 checksum matches the file's SHA1 checksum; false otherwise.
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    public static boolean verifyChecksum(String file, String testChecksum, Consumer<Integer> statusUpdate) throws NoSuchAlgorithmException, IOException
    {
        MessageDigest sha1 = MessageDigest.getInstance("SHA1");
        FileInputStream fis = new FileInputStream(file);


        final long length=new File(file).length();
        Consumer<Integer> performStatusUpdate = (percent) -> {
            if (statusUpdate!=null)
                statusUpdate.accept(percent);
        };
        long totalBytesRead = 0;
        byte[] data = new byte[1024*64];
        int read = 0;
        while ((read = fis.read(data)) != -1) {
            totalBytesRead+=read;
            sha1.update(data, 0, read);
            performStatusUpdate.accept((int)((totalBytesRead*100)/length));
        };
        byte[] hashBytes = sha1.digest();

        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < hashBytes.length; i++) {
            sb.append(Integer.toString((hashBytes[i] & 0xff) + 0x100, 16).substring(1));
        }

        String fileHash = sb.toString();
        performStatusUpdate.accept(100);
        return fileHash.equals(testChecksum);
    }


}
