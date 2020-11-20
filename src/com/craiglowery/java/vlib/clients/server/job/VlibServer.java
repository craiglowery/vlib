package com.craiglowery.java.vlib.clients.server.job;

import java.util.Base64;


/**
 * Encompasses the URI and credentials necessary to access a vlib server endpoint.
 */
public class VlibServer {

    private VlibServerProfile profile;
    private String authheader="";

    public VlibServer(VlibServerProfile profile) {
        this.profile=profile;
        authheader = "Basic "+ Base64.getEncoder().encodeToString((profile.username+":"+profile.password).getBytes());
    }

    /**
     * Returns the root URI to the server without embedded credentials.
     * @return
     */
    public String getRootURI() {
        return profile.baseUrl;
    }

    public String getAuthheader() {
        return authheader;
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


}
