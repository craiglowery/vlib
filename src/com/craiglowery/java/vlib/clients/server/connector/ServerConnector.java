package com.craiglowery.java.vlib.clients.server.connector;

import com.craiglowery.java.common.Util;
import com.craiglowery.java.vlib.clients.core.NameValuePair;
import com.craiglowery.java.vlib.clients.core.Tag;
import com.craiglowery.java.vlib.clients.upload.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.WorkerStateEvent;
import org.apache.http.Header;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.*;
import java.io.*;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;


/**
 * Created by Craig on 2/2/2016.
 */
public class ServerConnector {
    
    public enum HttpMethod {GET, PUT, POST, DELETE};
    
    private static String ROOTURI = null;
    private DocumentBuilderFactory documentBuilderFactory =null;

    private String username="";
    private String password="";
    private String authheader="";

    public ServerConnector(String RootUri, String username, String password) throws ServerException {
        ROOTURI=RootUri;
        documentBuilderFactory=DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setNamespaceAware(true);
        this.username=username;
        this.password=password;
        authheader = "Basic "+ Base64.getEncoder().encodeToString((username+":"+password).getBytes());
    }

    public String getROOTURI() {
        return getROOTURI(false);
    }

    public String getROOTURI(boolean embedCredentials) {
        if (embedCredentials)
            try {
                URIBuilder ub = new URIBuilder(ROOTURI);
                return ub.setUserInfo(username,password).toString();
            } catch (URISyntaxException e) {
            }
        return ROOTURI;
    }

    //---tag getting task-----------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<ObservableList<Tag>>> createTagNameGettingTask() {

        ServerConnectorTask<ServerResponse<ObservableList<Tag>>> task = new ServerConnectorTask<ServerResponse<ObservableList<Tag>>>() {
            @Override
            protected ServerResponse<ObservableList<Tag>> call() throws ServerException {
                return getTagNames(this);
            }
        };
        return task;
    }
    //---value getting task---------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<ObservableList<String>>> createTagValueGettingTask(String name) {

        ServerConnectorTask<ServerResponse<ObservableList<String>>> task = new ServerConnectorTask<ServerResponse<ObservableList<String>>>() {
            @Override
            protected ServerResponse<ObservableList<String>> call() throws ServerException {
                return getTagValues(this,name);
            }
        };
        return task;
    }

    //---complete tag definition getting task---------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<Document>> createTagDefinitionsGettingTask() {

        ServerConnectorTask<ServerResponse<Document>> task = new ServerConnectorTask<ServerResponse<Document>>() {
            @Override
            protected ServerResponse<Document> call() throws ServerException {
                return getTagDefinitions(this);
            }
        };
        return task;
    }

    //---upload task----------------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<UploadResult>>
        createFileUploadTask(String filename,
                             String title,
                             String contenttype,
                             String shandle,
                             boolean duplicateCheck,
                             List<NameValuePair> tags,
                             boolean autocreate) {
        ServerConnectorTask<ServerResponse<UploadResult>> task = new ServerConnectorTask<ServerResponse<UploadResult>>() {
            @Override
            protected ServerResponse call() throws ServerException {
                return doUpload(this,filename,title,contenttype, shandle,duplicateCheck, tags, autocreate);
            }
        };
        return task;
    }


    //---update task----------------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<Document>>
    createUpdateTask(Collection<UpdateRequest> requests) {
        ServerConnectorTask<ServerResponse<Document>> task = new ServerConnectorTask<ServerResponse<Document>>() {
            @Override
            protected ServerResponse call() throws ServerException {
                return doUpdate(this,requests);
            }
        };
        return task;
    }
    //---query task----------------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<Document>>
    createQueryTask(String select,
                    String where,
                    String orderby) {
        ServerConnectorTask<ServerResponse<Document>> task = new ServerConnectorTask<ServerResponse<Document>>() {
            @Override
            protected ServerResponse call() throws ServerException {
                return doQuery(this,select,where,orderby);
            }
        };
        return task;
    }
    //---titlehash task----------------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<Document>>
    createTitleHashTask(String select,
                    String title,
                    String orderby) {
        ServerConnectorTask<ServerResponse<Document>> task = new ServerConnectorTask<ServerResponse<Document>>() {
            @Override
            protected ServerResponse call() throws ServerException {
                return doTitleHash(this,select,title,orderby);
            }
        };
        return task;
    }
    //---schema task----------------------------------------------------------------------------------------------------
    public ServerConnectorTask<ServerResponse<Document>> createSchemaTask() {
        ServerConnectorTask<ServerResponse<Document>> task = new ServerConnectorTask<ServerResponse<Document>>() {
            @Override
            protected ServerResponse call() throws ServerException {
                return doSchema(this);
            }
        };
        return task;
    }


    //------------------------------------------------------------------------------------------------------------------
    // SUPPORTING METHODS
    //------------------------------------------------------------------------------------------------------------------

    public ServerResponse<ObservableList<Tag>> getTagNames(ServerConnectorTask<?> t) throws ServerException {
        Logger l = new Logger(t);
        try {
            XPath xp = XPathFactory.newInstance().newXPath();
            l.status("Getting tag names from server");
            String baseUri = ROOTURI + "/tags?excludevalues=true";
            l.log("Base URI=" + baseUri);
            ObservableList<Tag> tags = FXCollections.observableArrayList(new ArrayList<>());
            RepositoryResponse rr = getXmlResponse(HttpMethod.GET, baseUri, null);
            if (rr.xml == null)
                throw new ServerException("No xml was returned", rr);
            rr.enforce200(Util.xmlDocumentToString(rr.xml));
            l.status("Parsing response");
            try {
                NodeList nodes = (NodeList) (xp.compile("/result/tags/tag")).evaluate(rr.xml, XPathConstants.NODESET);
                XPathExpression expName = xp.compile("@name");
                XPathExpression expType = xp.compile("@type");
                XPathExpression expDescription = xp.compile("@description");
                XPathExpression expBrowsingPriority = xp.compile("@browsing_priority");
                for (int x = 0; x < nodes.getLength(); x++) {
                    Node node = nodes.item(x);
                    String name = (String) expName.evaluate(node, XPathConstants.STRING);
                    String type = (String) expType.evaluate(node, XPathConstants.STRING);
                    String description = (String) expDescription.evaluate(node, XPathConstants.STRING);
                    String browsingPriority = (String) expBrowsingPriority.evaluate(node, XPathConstants.STRING);
                    tags.add(new Tag(name, description, type, browsingPriority));
                }
            } catch (XPathExpressionException e) {
                throw new ServerException("XPath error", e);
            }

            l.status("Returning");
            return new ServerResponse<ObservableList<Tag>>(tags, l.toString());

        } catch (Exception e) {
            throw throwNormalizeException(l,e);
        }
    }

    public ServerResponse<ObservableList<String>> getTagValues(ServerConnectorTask<ServerResponse<ObservableList<String>>> t, String name) throws ServerException {
        Logger l = new Logger(t);
        try {
            name = (name == null) ? "" : name.trim();
            if (name.equals(""))
                throw new ServerException("A tag name was not specified");
            l.status("Getting tag values for tag name '" + name + "'");
            ObservableList<String> values = FXCollections.observableArrayList(new ArrayList<>());
            XPath xp = XPathFactory.newInstance().newXPath();
            String uri = null;
            try {
                URIBuilder ub = new URIBuilder(ROOTURI);
                uri = new URIBuilder().setScheme(ub.getScheme()).setHost(ub.getHost()).setPort(ub.getPort()).setPath(ub.getPath() + "/tags/" + name).build().toString();
            } catch (URISyntaxException e) {
                throw new Exception("Unexpected URI syntax error", e);
            }
            l.log("Uri is " + uri);
            RepositoryResponse rr = getXmlResponse(HttpMethod.GET, uri, null);
            l.status("Parsing response");
            if (rr.xml == null)
                throw (new ServerException("No xml was returned", rr));
            rr.enforce200(rr.xml); //Throws ServerException drectly out
            NodeList nodes = (NodeList) xp.compile("/result/tag/value").evaluate(rr.xml, XPathConstants.NODESET);
            for (int x = 0; x < nodes.getLength(); x++) {
                values.add(nodes.item(x).getTextContent());
            }
            l.status("Returning");
            return new ServerResponse<ObservableList<String>>(values, l.toString());
        } catch (Exception e) {
            throw throwNormalizeException(l,e);
        }
    }


    public ServerResponse<UploadResult> doUpload(
            ServerConnectorTask<ServerResponse<UploadResult>> t,
            String filename,
            String title,
            String contenttype,
            String shandle,
            boolean duplicateCheck,
            List<NameValuePair> tags,
            boolean autoCreate)
            throws ServerException {
        Logger l = new Logger(t);
        try {
            l.setPrintSream(System.err);
            l.status("Uploading " + filename);
            String baseUri = ROOTURI + "/upload";
            RepositoryResponse rr = null;
            l.log("Base URI is " + baseUri);
            String key = "";
            //Open the file
            l.log("Opening file " + filename);
            XPath xp = XPathFactory.newInstance().newXPath();
            String local_checksum = null;
            try (FileInputStream fis = new FileInputStream(filename)) {
                //Create the UR
                l.log("Creating Upload Resource");
                l.log(">>------------------------------------------------------------");
                try {
                    String ub = baseUri;
                    long filesize = new File(filename).length();
                    //Add textField_Handle to URI if specified
                    if (shandle != null && !(shandle = shandle.trim()).equals("")) {
                        try {
                            int h = Integer.parseInt(shandle);
                            if (h > 0)
                                ub = String.format("%s?handle=%d&size=%d", baseUri, h,filesize);
                            else
                                throw new ServerException("Handle must be a positive integer: '%s'",shandle);
                        } catch (NumberFormatException e) {
                            throw new ServerException("Handle must be a positive integer - null or empty string provided");
                        }
                    } else {
                        ub = String.format("%s?size=%d", baseUri, filesize);
                    }

                    rr = getXmlResponse(HttpMethod.POST, ub, null);
                    rr.enforce200(rr.xml);
                } catch (ServerException e) {
                   throw e;
                } catch (Exception e) {
                    throw new ServerException("could not create upload resource", rr, e);
                }
                l.log(rr);
                l.log("<<------------------------------------------------------------");
                l.status("Parsing UR creation response");
                l.log("Getting the key");
                //Get the key
                key = (String) (xp.compile("/result/upload/key/text()").evaluate(rr.xml, XPathConstants.STRING));
                if (key == null)
                    throw new ServerException("Could not retrieve key from UR XML", rr);
                l.log("The key is: " + key);
                l.status("Copying chunks");
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

                l.log("The buffer size is %d.  The file size is %d.  %d chunks to be transfered.", BUFSZ, filesize, numChunks);
                rr = null;
                while ((bytesread = fis.read(buf)) >= 0) {
                    if (bytesread == 0)
                        throw new ServerException("read 0 bytes from file - this should not happen!");
                    String uri = String.format("%s/%s/%d", baseUri, key, offset);
                    l.status("Transferring chunk #%d of %d", ++chunk, numChunks);
                    final int br = bytesread;
                    sha1Digest.update(buf, 0, bytesread);
                    rr = null;
                    try {
                        l.log(">>------------------------------------------------------------");
                        //Send the bytes to the serve
                        rr = putChunk(uri, buf, bytesread);
                        l.log(rr);
                        l.log("<<------------------------------------------------------------");
                        rr.enforce200(rr.xml);
                    } catch (ServerException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new ServerException("Error while posting data chunk", rr, e);
                    }
                    offset += bytesread;
                    t.progress(offset, filesize);
                    //thread.join();
                }
                t.progress(-1, 1);
                l.status("Transfer complete");
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
            l.log("Local SHA1 digest checksum = %s", local_checksum);
            l.status("Requesting checksum");
            l.log(">>------------------------------------------------------------");
            String uri = String.format("%s/%s?computechecksum=yes", baseUri, key);
            l.log("Uri for checksum is %s", uri);

            try {
                rr = getXmlResponse(HttpMethod.GET, uri, null);
                String remote_checksum = ((String) xp.evaluate("//checksum[1]/text()", rr.xml, XPathConstants.STRING)).toLowerCase();
                l.log("Remote SHA1 digest checksm = %s", remote_checksum);
                if (!local_checksum.equals(remote_checksum))
                    throw new ServerException("Checksum mismatch:\n  Local= %s\n  Remote=%s", local_checksum, remote_checksum);
                rr.enforce200(rr.xml);
            } catch (ServerException e) {
                throw e;
            } catch (Exception e) {
                throw new ServerException("Checksum request failed", e);
            }
            l.log(rr);

            l.log("<<------------------------------------------------------------");

            //Modify the UR for filename and title - POST
            l.status("Creating finalization document");
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
            elContenttype.setTextContent(contenttype);
            elUpload.appendChild(elContenttype);

            doc.appendChild(elUpload);

            l.log("Posting finalize document");
            try {
                l.log(prettyXml(doc));
            } catch (Exception e) {
                l.log("Unable to prettyprint xml for log: " + e.getMessage());
            }
            uri = String.format("%s/%s?duplicatecheck=%s", baseUri, key, duplicateCheck ? "yes" : "no");
            l.log("The Uri for finalization is %s", uri);
            l.status("Posting finalization request");
            l.log(">>------------------------------------------------------------");

            rr = null;
            try {
                rr = getXmlResponse(HttpMethod.POST, uri, doc);
                rr.enforce200(rr.xml);
            } catch (ServerException e) {
                throw e;
            } catch (Exception e) {
                throw new ServerException("Finalize request failed", rr, e);
            }
            l.log(rr);
            l.log("<<------------------------------------------------------------");
            //get the textField_Handle
            if (rr == null || rr.xml == null)
                throw new ServerException("No content returned from finalize", rr);
            int handle = -1;
            try {
                String s = (String) xp.compile("/result/object/@handle").evaluate(rr.xml);
                handle = Integer.parseInt(s);
            } catch (XPathExpressionException e) {
                throw new ServerException("Could not parse XML for textField_Handle - upload MAY have been successful", rr);
            } catch (NumberFormatException e) {
                throw new ServerException("Could not parse textField_Handle value - upload MAY have been successful", rr);
            }
            l.status("Completed. Object textField_Handle is %d.", handle);
            UploadResult result = new UploadResult(handle);
            for (NameValuePair pair : tags) {
                l.log("---\nTagging with name='%s' Value='%s'", pair.name, pair.value);
                try {
                    //URI is  ROOT/objects/{handle}/tags/tag/{name}/{value}?autocreate=true|false
                    URIBuilder ub = new URIBuilder(ROOTURI);
                    uri = new URIBuilder()
                            .setScheme(ub.getScheme())
                            .setHost(ub.getHost())
                            .setPort(ub.getPort())
                            .setPath(String.format("%s/objects/%d/tags/tag/%s/%s", ub.getPath(), handle, pair.name, pair.value))
                            .addParameter("autocreate", autoCreate ? "true" : "false")
                            .build()
                            .toString();
                    l.log(">>------------------------------------------------------------");
                    rr = getXmlResponse(HttpMethod.PUT, uri, null);
                    l.log(">>------------------------------------------------------------");
                } catch (ServerException e) {
                    l.log("Tagging failed for %s=%s - %s",pair.name,pair.value,e.getMessage());
                    l.log(e);
                    result.tagFailed(pair);
                    continue;
                } catch (URISyntaxException e) {
                    l.log("Unable to tag %s=%s due to URI syntax error", pair.name, pair.value);
                    l.log(e);
                    result.tagFailed(pair);
                    continue;
                } catch (Exception e) {
                    l.log("Unexpected exception");
                    l.log(e);
                    result.tagFailed(pair);
                    l.log("Tagging failed for %s=%s", pair.name, pair.value);
                    continue;
                }
                if (rr.statusLine.getStatusCode() != 200 && rr.statusLine.getStatusCode() != 201) {
                    l.log("Tagging failed for %s=%s", pair.name, pair.value);
                    l.log(rr);
                    result.tagFailed(pair);
                } else {
                    l.log("Tagging for %s=%s succeeded", pair.name, pair.value);
                    result.tagSuccessful(pair);
                }
            }
            return new ServerResponse<UploadResult>(result, l.toString());
        } catch (Exception e) {
            throw throwNormalizeException(l,e);
        }
    }

    public class UpdateRequest {
        public long handle;
        public Collection<NameValuePair> tags;
    }

    public UpdateRequest createUpdateRequest(long handle,Collection<NameValuePair> tags) {
        UpdateRequest r = new UpdateRequest();
        r.handle=handle;
        r.tags = tags;
        return r;
    }

    public ServerResponse<Document> doUpdate(ServerConnectorTask<ServerResponse<Document>> t, Collection<UpdateRequest> requests) throws ServerException {
        t.progress(0,requests.size());
        Logger l = new Logger(t);
        try {
            Document xml;
            try {
                xml = documentBuilderFactory.newDocumentBuilder().newDocument();
            } catch (ParserConfigurationException e) {
                throw new ServerException("Unable to create XML document", e);
            }

            Element elReport = xml.createElement("update_report");
            xml.appendChild(elReport);
            Element elSuccess = xml.createElement("succeeded");
            Element elFailed = xml.createElement("failed");
            int success = 0;
            int failure = 0;
            for (UpdateRequest request : requests) {
                Element elHandle = xml.createElement("handle");
                elHandle.setTextContent(String.valueOf(request.handle));
                try {
                    doUpdateAux(l, request.handle, request.tags);
                    success++;
                    elSuccess.appendChild(elHandle);
                } catch (ServerException e) {
                    l.log(String.format("Update for handle %d failed - continuing to next update", request.handle));
                    failure++;
                    elHandle.setAttribute("failure_reason", e.getRepositoryResponse().getResponseSummary());
                    elFailed.appendChild(elHandle);
                }
                t.progress(success + failure, requests.size());
            }
            if (success > 0)
                elReport.appendChild(elSuccess);
            if (failure > 0)
                elReport.appendChild(elFailed);
            elReport.setAttribute("succeeded", String.valueOf(success));
            elReport.setAttribute("failed", String.valueOf(failure));

            return new ServerResponse<>(xml, l.toString());
        } catch (Exception e) {
           throw throwNormalizeException(l,e);
        }
    }

    /**
     * Takes an exception, normalizes it and uses a logger to log and throw it as a ServerException.
     * If the exception is not a ServerException, it is wrapped in one. In all cases, the logged and thrown
     * ServerException has the prepend value set to that of the calling method.
     * @param l The logger to use in logAndThrow
     * @param e  the exception to normalize.
     * @return The method does not return, but advertises that is returns a ServerException so that it can be
     * use to assure the compiler that a non-return is ensured (write "throw throwNormalizedException").
     */
    public ServerException throwNormalizeException(Logger l, Exception e) throws ServerException {
        ServerException se;
        if (!(e instanceof ServerException)) {
            se = new ServerException("Unexpected exception",e);
        } else {
            se = (ServerException) e;
        }
        Exception tracer = new Exception();
        se.prepend(tracer.getStackTrace()[1].getMethodName());
        l.logAndThrow(se);
        return se;  //never happens, but satisfies compiler
    }

    private void doUpdateAux(Logger l, long handle, Collection<NameValuePair> tags) throws Exception {
        l.status("Creating XML document for handle %s", handle);
        Document ux = documentBuilderFactory.newDocumentBuilder().newDocument();
        Element elObject = ux.createElement("object");
        elObject.setAttribute("handle", String.valueOf(handle));
        ux.appendChild(elObject);

        Element elVersions = ux.createElement("versions");

        Element elVersion = ux.createElement("version");
        elVersion.setAttribute("current", "true");
        elVersions.appendChild(elVersion);

        Element elAttributes = ux.createElement("attributes");
        elVersion.appendChild(elAttributes);

        Element elTags = ux.createElement("tags");

        boolean hasAttributes = false;
        boolean hasTags = false;

        for (NameValuePair nvp : tags) {
            switch (nvp.name.toLowerCase()) {
                case "title":
                    Element elTitle = ux.createElement("title");
                    elTitle.setTextContent(nvp.value);
                    elAttributes.appendChild(elTitle);
                    hasAttributes = true;
                    break;
                default /*tag*/:
                    Element elTag = ux.createElement("tag");
                    elTag.setAttribute("name", nvp.name);
                    elTag.setAttribute("value", nvp.value);
                    elTags.appendChild(elTag);
                    hasTags = true;
                    break;
            }
        }

        if (hasTags)
            elVersion.appendChild(elTags);
        elObject.appendChild(elVersions);


        //The document is assembled. Now we send it

        String baseUri = ROOTURI + "/objects/" + String.valueOf(handle) + "?autocreate=true";
        RepositoryResponse rr = null;
        l.log("Base URI is " + baseUri);

        //Create the query

        URIBuilder builder = null;
        builder = new URIBuilder(baseUri);
        String query = builder.build().toString();
        l.log("The request is: " + query);
        l.log("Sending");
        //Util.printXmlDocument(ux,System.err);
        try {
            rr = getXmlResponse(HttpMethod.PUT, query, ux);
        } catch (ServerException e) {
            throw e;
        } catch (Exception e) {
            throw new ServerException("Schema retrieval request failed", e);
        }
        rr.enforce200(rr.xml);
        l.status("Update complete");
    }

    public ServerResponse<Document> doSchema(ServerConnectorTask<ServerResponse<Document>> t) throws ServerException {
        Logger l = new Logger(t);
        try {
            l.status("Retrieving schema");
            String baseUri = ROOTURI + "/objects/schema";
            RepositoryResponse rr = null;
            l.log("Base URI is " + baseUri);

            //Create the query

            URIBuilder builder = null;
            builder = new URIBuilder(baseUri);
            String query = builder.build().toString();
            l.log("The request is: " + query);
            l.log("Sending");
            rr = getXmlResponse(HttpMethod.GET, query, null);
            rr.enforce200(rr.xml);
            l.status("Schema retrieval complete");
            return new ServerResponse<Document>(rr.xml, l.toString());
        } catch (Exception e) {
            throw throwNormalizeException(l, e);
        }
    }


    public ServerResponse<Document> doQuery(
            ServerConnectorTask<ServerResponse<Document>> t,
            String select,
            String where,
            String orderby)
            throws ServerException {
        Logger l = new Logger(t);
        try {
            l.status("Querying");
            String baseUri = ROOTURI + "/query";
            RepositoryResponse rr = null;
            l.log("Base URI is " + baseUri);

            //Create the query

            URIBuilder builder = null;
            builder = new URIBuilder(baseUri);
            if (select != null && select.length() > 0)
                builder.addParameter("select", select);
            if (where != null && where.length() > 0)
                builder.addParameter("where", where);
            if (orderby != null && orderby.length() > 0)
                builder.addParameter("orderby", orderby);
            builder.addParameter("includetags", "yes");
            String query = builder.build().toString();
            l.log("The parameterized request is: " + query);
            l.log("Sending");
            try {
                rr = getXmlResponse(HttpMethod.GET, query, null);
            } catch (ServerException e) {
                throw e;
            } catch (Exception e) {
                throw new ServerException("Query request failed", e);
            }
            rr.enforce200(rr.xml);
            l.status("Query complete");
            return new ServerResponse<Document>(rr.xml, l.toString());
        } catch(Exception e) {
            throw throwNormalizeException(l,e);
        }
    }

    public ServerResponse<Document> doTitleHash(
            ServerConnectorTask<ServerResponse<Document>> t,
            String select,
            String title,
            String orderby)
            throws ServerException {
        Logger l = new Logger(t);
        try {
            l.status("Querying title hash");
            String baseUri = ROOTURI + "/query/title";
            RepositoryResponse rr = null;
            l.log("Base URI is " + baseUri);

            //Create the query

            URIBuilder builder = null;
            builder = new URIBuilder(baseUri);
            if (select != null && select.length() > 0)
                builder.addParameter("select", select);
            if (title != null && title.length() > 0)
                builder.addParameter("title", title);
            if (orderby != null && orderby.length() > 0)
                builder.addParameter("orderby", orderby);
            builder.addParameter("quick", "no");
            String query = builder.build().toString();
            l.log("The parameterized request is: " + query);
            l.log("Sending");
            try {
                rr = getXmlResponse(HttpMethod.GET, query, null);
            } catch (ServerException e) {
                throw e;
            } catch (Exception e) {
                throw new ServerException("Query request failed", e);
            }
            rr.enforce200(rr.xml);
            l.status("Query complete");
            return new ServerResponse<Document>(rr.xml, l.toString());
        } catch(Exception e) {
            throw throwNormalizeException(l,e);
        }
    }

    /**
     * Performs an asynchronous query to the server.
     *
     * @param select The comma separated list of attributes to select, or null for all attributes.
     * @param where The filter expression, or null for all objects.
     * @param orderby The comma separated list of fields to sort by, or null for the default ordering.
     * @param success References a function that consumes an XML Document (the response).  The format of
     *                the document is:
     * <PRE>{@code
     *   <query select=”attributelist” where=”filterexpression” orderby=”attributelist” includetags=”yes|no”></query>
            <objects>
                <object>
                    <attributes>
                        <attributename>value<attributename> ...
                    </attributes>
                    <tags>
                        <tag name=”name” value=”value”/> ...
                    <tags>
                </object>
            </objects>
         </query>
    }</PRE>


 occur.
     */
    public void callAsyncQuery(
            String select,
            String where,
            String orderby,
            Consumer<Document> success,
            Consumer<Throwable> failure
            )
    {
        //Create the task
        ServerConnectorTask<ServerResponse<Document>> queryTask =
                this.createQueryTask(select,where,orderby);

        //Set up listeners for when the background thread completes.
        java.util.function.BiConsumer<WorkerStateEvent,Boolean>cleanup = (event, succeeded) -> {
            if (succeeded) {
                success.accept(queryTask.getValue().getResult());
            } else {
                Throwable e = queryTask.getException();
                failure.accept(e);
            }


        };

        queryTask.setOnSucceeded(event -> cleanup.accept(event,true));
        queryTask.setOnFailed(event -> cleanup.accept(event,false));

        //Schedule the task and let 'er run!
        Thread backgroundThread = new Thread(queryTask);
        backgroundThread.setDaemon(true);
        backgroundThread.start();

    }


    public ServerResponse<Document> getTagDefinitions(ServerConnectorTask<ServerResponse<Document>> t)
            throws ServerException {
        Logger l = new Logger(t);
        try {
            l.status("Retrieving tag definitions");
            String baseUri = ROOTURI + "/tags";
            RepositoryResponse rr = null;
            l.log("Base URI is " + baseUri);

            URIBuilder builder = null;
            builder = new URIBuilder(baseUri);
            builder.addParameter("excludevalues", "false");
            String query = builder.build().toString();
            l.log("The parameterized request is: " + query);
            l.log("Sending");
            rr = getXmlResponse(HttpMethod.GET, query, null);
            rr.enforce200("While getting tag definitions");
            l.status("Query complete");
            return new ServerResponse<Document>(rr.xml, l.toString());
        } catch (Exception e) {
            throw throwNormalizeException(l,e);
        }
    }



    CloseableHttpClient httpClient = HttpClients.createDefault();

    public static String prettyXml(Document xml) throws Exception {
        try (Writer out = new StringWriter()) {
            Transformer tf = TransformerFactory.newInstance().newTransformer();
            tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            tf.setOutputProperty(OutputKeys.INDENT, "yes");
            tf.transform(new DOMSource(xml), new StreamResult(out));
            return out.toString();
        } catch (Exception e) {
            throw new Exception("error while pretty-printing XML document", e);
        }
    }

    /**
     * Executes an HTTP request with optional XML content, returning an object
     * holding any returned XML document, the response status and headers.
     *
     * @param method  One of GET, PUT, POST, or DELETE
     * @param uri     The resource to target
     * @param payload Optional XML payload to send.
     * @return The salient response components.
     * @throws Exception
     */
    public RepositoryResponse getXmlResponse(HttpMethod method, String uri, Document payload) throws ServerException {
        HttpUriRequest request = null;
        switch (method) {
            case DELETE:
                request = new HttpDelete(uri);
                break;
            case GET:
                request = new HttpGet(uri);
                break;
            case PUT:
                HttpPut put = new HttpPut(uri);
                put.setEntity(new InputStreamEntity(createInputStream(payload), ContentType.APPLICATION_XML));
                request = put;
                break;
            case POST:
                HttpPost post = new HttpPost(uri);
                post.setEntity(new InputStreamEntity(createInputStream(payload), ContentType.APPLICATION_XML));
                request = post;
                break;
        }


        return executeRequest(request);
    }

    RepositoryResponse putChunk(String uri, byte[] buf, int count) throws Exception {
        HttpPut put = new HttpPut(uri);
        try (InputStream is = new ByteArrayInputStream(buf, 0, count)) {
            put.setEntity(new ByteArrayEntity(buf, 0, count, ContentType.APPLICATION_OCTET_STREAM));
            return executeRequest(put);
        } catch (IOException e) {
            throw new Exception("IOerror while putting chunk", e);
        }
    }

    RepositoryResponse executeRequest(HttpUriRequest request) throws ServerException {
        request.addHeader("Authorization",authheader);
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            //Let's get any XML there might be, and the statusCode
            Header[] contentTypeHeaders = response.getHeaders("Content-Type");
            Document doc = null;
            if (contentTypeHeaders!=null && contentTypeHeaders[0].getValue().startsWith("application/xml")) {
                try {
                    doc = getDocumentBuilder().parse(response.getEntity().getContent());
                } catch (Exception e) {
                    throw new ServerException("An error occurred while reading/parsing XML content from the server.", e);
                }
            }
            return new RepositoryResponse(doc, response.getStatusLine(), response.getAllHeaders());
        } catch (IOException e) {
            throw new ServerException("Error closing HTTP response object", e);
        }

    }

    public InputStream createInputStream(Document doc) throws ServerException {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Source xmlSource = new DOMSource(doc);
            Result outputTarget = new StreamResult(outputStream);
            TransformerFactory.newInstance().newTransformer().transform(xmlSource, outputTarget);
            return new ByteArrayInputStream(outputStream.toByteArray());
        } catch (Exception e) {
            throw new ServerException("Could not create stream for outbound XML document", e);
        }
    }




    private DocumentBuilder getDocumentBuilder() throws Exception {
        synchronized (documentBuilderFactory) {
            return documentBuilderFactory.newDocumentBuilder();
        }
    }



}
