package com.craiglowery.java.vlib.clients.upload;

import com.craiglowery.java.vlib.clients.core.NameValuePair;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Craig on 2/5/2016.
 */
public class UploadResult {
    int handle;
    List<NameValuePair> tagsSuccessful = new LinkedList<NameValuePair>();
    List<NameValuePair> tagsFailed = new LinkedList<NameValuePair>();

    public UploadResult(int handle) {
        this.handle=handle;
    }

    public int getHandle() {
        return handle;
    }

    public List<NameValuePair> getTagsSuccessful() {
        return tagsSuccessful;
    }

    public List<NameValuePair> getTagsFailed() {
        return tagsFailed;
    }

    public void tagFailed(NameValuePair pair) {
        tagsFailed.add(pair);
    }

    public void tagSuccessful(NameValuePair pair) {
        tagsSuccessful.add(pair);
    }

    public boolean hadFailedTags() {
        return tagsFailed.size()!=0;
    }

    public String statusLine() {
        return String.format("%s handle=%d",
                hadFailedTags()?"UPLOAD OK, SOME TAGS FAILED":"SUCCESS",
                handle);    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(statusLine()).append("\n")
          .append(String.format("Tags applied successfuly: %d of %d\n",tagsSuccessful.size(),tagsSuccessful.size()+tagsFailed.size()));
        for (NameValuePair pair : tagsSuccessful) {
            sb.append(String.format("  '%s'='%s'\n",pair.name,pair.value));
        }
        sb.append(String.format("Tags failing: %d of %d\n",tagsFailed.size(),tagsSuccessful.size()+tagsFailed.size()));
        for (NameValuePair pair : tagsFailed) {
            sb.append(String.format("  '%s'='%s'\n",pair.name,pair.value));
        }
        return sb.toString();
    }

}
