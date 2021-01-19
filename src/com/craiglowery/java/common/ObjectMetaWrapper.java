package com.craiglowery.java.common;

import com.mashape.unirest.http.ObjectMapper;
import com.mashape.unirest.http.Unirest;
import org.json.JSONObject;

public class ObjectMetaWrapper {

    JSONObject jsonRoot;
    JSONObject jsonVersion;

    static {
        Unirest.setObjectMapper(new ObjectMapper() {
            com.fasterxml.jackson.databind.ObjectMapper mapper
                    = new com.fasterxml.jackson.databind.ObjectMapper();

            public String writeValue(Object value) {
                try {
                    return mapper.writeValueAsString(value);
                } catch (Exception e) {
                    throw new Error(e);
                }
            }

            public <T> T readValue(String value, Class<T> valueType) {
                try {
                    return mapper.readValue(value, valueType);
                } catch (Exception e) {
                    throw new Error(e);
                }
            }
        });
    }

    public ObjectMetaWrapper(String jsonText)  {

        jsonRoot = new JSONObject(jsonText);
        jsonVersion = jsonRoot.getJSONObject("version");
    }

    public int getHandle() {
        return jsonVersion.getInt("handle");
    }

    public String getImported() {
        return jsonVersion.getString("imported");
    }

    public String getSha1sum() { return jsonVersion.getString("sha1sum"); }

}
