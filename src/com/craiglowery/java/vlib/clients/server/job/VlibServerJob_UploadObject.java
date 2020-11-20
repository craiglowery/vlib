package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.vlib.clients.core.NameValuePair;
import com.craiglowery.java.vlib.clients.server.connector.*;
import com.craiglowery.java.vlib.clients.upload.UploadResult;
import org.apache.http.client.utils.URIBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.List;

public class VlibServerJob_UploadObject extends VlibServerJob<UploadResult>{

    //----CONSTRUCTOR---------------------------------------------------------------

    public VlibServerJob_UploadObject(
            String filename,
            String title,
            String contentType,
            String sHandle,
            boolean duplicateCheck,
            List<NameValuePair> tags,
            boolean autoCreate
    ) {
        super("Upload file "+title+" ("+filename+")");
        this.filename=filename;
        this.title=title;
        this.contentType=contentType;
        this.sHandle=sHandle;
        this.duplicateCheck=duplicateCheck;
        this.tags=tags;
        this.autoCreate=autoCreate;
    }

    private String filename;
    private String title;
    private String contentType;
    private String sHandle;
    private boolean duplicateCheck;
    private List<NameValuePair> tags;
    private boolean autoCreate;

    @Override
    protected UploadResult computeDataResult() throws VlibServerException {
        UploadResult result=null;
        try {
            setServerJobStatus("Uploading " + filename);
            String baseUri = "/upload";
            logger.log("Base URI is "+baseUri);
            VlibRepositoryResponse rr = null;
            String key = "";

            //Open the file
            logger.log("Opening file " + filename);
            XPath xp = XPathFactory.newInstance().newXPath();
            String local_checksum = null;
            try (FileInputStream fis = new FileInputStream(filename)) {
                //Create the UR
                logger.log("Creating Upload Resource");
                try {
                    String ub = baseUri;
                    long filesize = new File(filename).length();
                    //Add textField_Handle to URI if specified
                    if (sHandle != null && !(sHandle = sHandle.trim()).equals("")) {
                        try {
                            int h = Integer.parseInt(sHandle);
                            if (h > 0)
                                ub = String.format("%s?handle=%d&size=%d", baseUri, h,filesize);
                            else
                                throw new ServerException("Handle must be a positive integer: '%s'",sHandle);
                        } catch (NumberFormatException e) {
                             throw new ServerException("Handle must be a positive integer - null or empty string provided");
                        }
                    } else {
                         ub = String.format("%s?size=%d", baseUri, filesize);
                    }

                    rr = getXmlResponse(HttpMethod.POST, ub, null);
                    rr.enforce200(rr.xml);
                } catch (VlibServerException e) {
                    throw e;
                } catch (Exception e) {
                    throw new VlibServerException("could not create upload resource", rr, e);
                }
                logger.log(rr);
                setServerJobStatus("Parsing UR creation response");
                logger.log("Getting the key");

                //Get the key
                key = (String) (xp.compile("/result/upload/key/text()").evaluate(rr.xml, XPathConstants.STRING));
                if (key == null)
                    throw new ServerException("Could not retrieve key from UR XML", rr);
                logger.log("The key is: " + key);
                setServerJobStatus("Copying chunks");

                //Read 5MB chunks and send, also computing checksum
                final int BUFSZ = 1024 * 1024 * 5;
                final byte[] buf = new byte[BUFSZ];
                int bytesread = 0;
                long offset = 0;
                int chunk = 0;
                File sourceFile = new File(filename);
                long filesize = sourceFile.length();
                int numChunks = (int) (filesize / BUFSZ);
                if (filesize % BUFSZ > 0)
                    numChunks++;

                //MessageDigest sha1Digest = org.apache.commons.codec.digest.DigestUtils.getSha1Digest();
                MessageDigest sha1Digest = MessageDigest.getInstance("SHA-1");

                logger.log("The buffer size is %d.  The file size is %d.  %d chunks to be transfered.", BUFSZ, filesize, numChunks);
                rr = null;
                while ((bytesread = fis.read(buf)) >= 0) {
                    if (bytesread == 0)
                        throw new ServerException("read 0 bytes from file - this should not happen!");
                    String uri = String.format("%s/%s/%d", baseUri, key, offset);
                    setServerJobStatus("Transferring chunk #%d of %d", ++chunk, numChunks);
                    final int br = bytesread;
                    sha1Digest.update(buf, 0, bytesread);
                    rr = null;
                    try {
                        //Send the bytes to the serve
                        rr = putChunk(uri, buf, bytesread);
                        logger.log(rr);
                        rr.enforce200(rr.xml);
                    } catch (VlibServerException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new VlibServerException("Error while posting data chunk", rr, e);
                    }
                    offset += bytesread;
                    setServerJobStatus(((int)((offset*100.0)/filesize))+"% uploaded");
                }
                setServerJobStatus("100% uploaded");
                setServerJobStatus("Transfer complete");
                StringBuffer sb = new StringBuffer();
                byte[] digest = sha1Digest.digest();
                for (byte c : digest) {
                    sb.append(String.format("%x%x", (c & 0xF0) >> 4, c & 0xF));
                }
                local_checksum = sb.toString();
            } catch (FileNotFoundException e) {
                throw new ServerException("Could not open source file", e);
            } catch (IOException e) {
                throw new ServerException("IO error", e);
            } catch (XPathExpressionException e) {
                throw new ServerException("Unexpected - XPath expression error", e);
            } catch (Exception e) {
                throw new ServerException("Error while uploading file", e);
            }
            //Get the UR and compare the checksum

            if (local_checksum == null)
                throw new ServerException("Local checksum not computed");
            logger.log("Local SHA1 digest checksum = %s", local_checksum);
            setServerJobStatus("Requesting checksum");
            String uri = String.format("%s/%s?computechecksum=yes", baseUri, key);
            logger.log("Uri for checksum is %s", uri);

            try {
                rr = getXmlResponse(HttpMethod.GET, uri, null);
                String remote_checksum = ((String) xp.evaluate("//checksum[1]/text()", rr.xml, XPathConstants.STRING)).toLowerCase();
                logger.log("Remote SHA1 digest checksm = %s", remote_checksum);
                if (!local_checksum.equals(remote_checksum))
                    throw new ServerException("Checksum mismatch:\n  Local= %s\n  Remote=%s", local_checksum, remote_checksum);
                rr.enforce200(rr.xml);
            } catch (VlibServerException e) {
                throw e;
            } catch (Exception e) {
                throw new ServerException("Checksum request failed", e);
            }
            logger.log(rr);


            //Modify the UR for filename and title - POST
            setServerJobStatus("Creating finalization document");
            Document doc = null;
            try {
                doc = getDocumentBuilder().newDocument();
            } catch (Exception e) {
                throw new ServerException("Error initializing XML document builder", e);
            }
            Element elUpload = doc.createElement("upload");

            Element elFilename = doc.createElement("filename");
            elFilename.setTextContent(new File(filename).getName());
            elUpload.appendChild(elFilename);

            Element elTitle = doc.createElement("title");
            elTitle.setTextContent(title);
            elUpload.appendChild(elTitle);

            Element elContenttype = doc.createElement("contenttype");
            elContenttype.setTextContent(contentType);
            elUpload.appendChild(elContenttype);

            doc.appendChild(elUpload);

            logger.log("Posting finalize document");
            try {
                logger.log(prettyXml(doc));
            } catch (Exception e) {
                logger.log("Unable to prettyprint xml for log: " + e.getMessage());
            }
            uri = String.format("%s/%s?duplicatecheck=%s", baseUri, key, duplicateCheck ? "yes" : "no");
            logger.log("The Uri for finalization is %s", uri);
            setServerJobStatus("Posting finalization request");

            rr = null;
            try {
                rr = getXmlResponse(HttpMethod.POST, uri, doc);
                rr.enforce200(rr.xml);
            } catch (VlibServerException e) {
                throw e;
            } catch (Exception e) {
                throw new VlibServerException("Finalize request failed", rr, e);
            }
            logger.log(rr);
            //get the textField_Handle
            if (rr == null || rr.xml == null)
                throw new VlibServerException("No content returned from finalize", rr);
            int handle = -1;
            try {
                String s = (String) xp.compile("/result/object/@handle").evaluate(rr.xml);
                handle = Integer.parseInt(s);
            } catch (XPathExpressionException e) {
                throw new VlibServerException("Could not parse XML for textField_Handle - upload MAY have been successful", rr);
            } catch (NumberFormatException e) {
                throw new VlibServerException("Could not parse textField_Handle value - upload MAY have been successful", rr);
            }
            setServerJobStatus("Completed. Object textField_Handle is %d.", handle);
            result = new UploadResult(handle);
            for (NameValuePair pair : tags) {
                logger.log("---\nTagging with name='%s' Value='%s'", pair.name, pair.value);
                try {
                //URI is  ROOT/objects/{handle}/tags/tag/{name}/{value}?autocreate=true|false
                uri = String.format("/objects/%d/tags/tag/%s/%s?autocreate=%s",
                        handle,
                        Util.encodeUrl(pair.name),
                        Util.encodeUrl(pair.value),
                        autoCreate ? "true" : "false");
                        rr = getXmlResponse(VlibServerJob.HttpMethod.PUT, uri, null);
                    } catch (VlibServerException e) {
                        logger.log("Tagging failed for %s=%s - %s",pair.name,pair.value,e.getMessage());
                        logger.log(e);
                        result.tagFailed(pair);
                        continue;
                    } catch (Exception e) {
                        logger.log("Unexpected exception");
                        logger.log(e);
                        result.tagFailed(pair);
                        logger.log("Tagging failed for %s=%s", pair.name, pair.value);
                        continue;
                    }
                    if (rr.statusLine.getStatusCode() != 200 && rr.statusLine.getStatusCode() != 201) {
                        logger.log("Tagging failed for %s=%s", pair.name, pair.value);
                        logger.log(rr);
                        result.tagFailed(pair);
                    } else {
                        logger.log("Tagging for %s=%s succeeded", pair.name, pair.value);
                        result.tagSuccessful(pair);
                    }
                }
            } catch (Exception e) {
                VlibServerException.throwNormalizedException(logger,e);
            }
            return result;
        }

}
