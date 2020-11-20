package com.craiglowery.java.vlib.clients.server.job;

import com.craiglowery.java.jobmgr.ExecuteUponReturn;
import com.craiglowery.java.jobmgr.Job;
import com.craiglowery.java.jobmgr.JobManager;
import org.apache.http.client.utils.URIBuilder;

import javax.xml.parsers.DocumentBuilderFactory;
import java.net.URISyntaxException;
import java.util.Base64;
import java.util.function.Consumer;

public class VlibServerJobManager extends JobManager<VlibServerJobResult,VlibServerJobManager> {

    VlibServer server;


    /**
     * Constructs a new JobManager and kicks off the scheduler thread.
     *
     * @param statusCallback     Called by the job manager whenever a job signals a status change. The job
     *                           object is passed back and can be examined for the status information
     *                           using {@code getLastStatus()}.
     * @param server             The vlib server that is the target for the jobs managed by this job manager.
     **/
    public VlibServerJobManager(
            Consumer<Job<VlibServerJobResult, VlibServerJobManager>> statusCallback,
            VlibServer server)

    {
        super(statusCallback);
        this.server=server;
    }

    public VlibServer getServer() {
        return server;
    }

}
