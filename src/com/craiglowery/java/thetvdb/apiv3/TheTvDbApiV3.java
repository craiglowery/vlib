package com.craiglowery.java.thetvdb.apiv3;
import com.craiglowery.java.common.Util;
import com.craiglowery.java.thetvdb.TheTvDb;
import com.craiglowery.java.thetvdb.TheTvDbEpisode;
import com.craiglowery.java.thetvdb.TheTvDbSeries;
import com.craiglowery.java.vlib.clients.core.NameValuePairList;
import com.craiglowery.java.vlib.clients.server.job.UploadServer;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.ObjectMapper;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.request.GetRequest;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Creates a session with thetvdb.com REST API V3.
 */

public class TheTvDbApiV3 implements TheTvDb {

    final String endpoint="https://api.thetvdb.com";

    String apikey;
    String username;
    String userkey;
    String JWT_Token;

    public TheTvDbApiV3(String apikey, String username, String userkey) throws Exception {
        this.apikey=apikey;
        this.username=username;
        this.userkey=userkey;

        Unirest.setObjectMapper(new ObjectMapper() {
            com.fasterxml.jackson.databind.ObjectMapper mapper
                    = new com.fasterxml.jackson.databind.ObjectMapper();

            public String writeValue(Object value) {
                try {
                    return mapper.writeValueAsString(value);
                } catch (Exception e) {
                    throw new Error("Unexpected mapper write error: "+e.getClass().getName()+": "+e.getMessage());
                }
            }

            public <T> T readValue(String value, Class<T> valueType) {
                try {
                    return mapper.readValue(value, valueType);
                } catch (Exception e) {
                    throw new Error("Unexpected mapper read error: "+e.getClass().getName()+": "+e.getMessage());
                }
            }
        });

        String requestBody =String.format(
                "{\"apikey\": \"%s\",\"username\": \"%s\",\"userkey\": \"%s\"}",
                apikey,username,userkey);

        HttpResponse<JsonNode> jsonResponse =
                Unirest.post(endpoint+"/login")
                    .header("Content-type","application/json")
                    .header("accept","application/json")
                    .body(requestBody)
                    .asJson();

        if (jsonResponse.getStatus()!=200)
            throw new Exception(
                    String.format("HTTP Error: Status=%d Body='%s'",
                            jsonResponse.getStatus(),
                            jsonResponse.getBody().toString()));

        try {
            Object token = (jsonResponse.getBody().getObject().get("token"));
            if (token == null)
                throw new Exception();
            JWT_Token = token.toString();
        } catch (Exception e) {
            throw new Exception(String.format("Error parsing JSON: Could not detect authentication token in '%s'",
                jsonResponse.getBody().toString()));
        }
    }

    @Override
    public List<TheTvDbEpisode> getEpisodes(long seriesId) throws Exception{
        int page = 1;
        boolean morePages=true;

        List<TheTvDbEpisode> list = new LinkedList<>();

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        BiFunction<JSONObject,String,Long> safeGetLong = (episode,key) -> {
            if (episode.isNull(key)) return 0L;
            return episode.getLong(key);
        };

        BiFunction<JSONObject,String,Integer> safeGetInt = (episode,key) -> {
            if (episode.isNull(key)) return 0;
            return episode.getInt(key);
        };

        BiFunction<JSONObject,String,String> safeGetString = (episode,key) -> {
            if (episode.isNull(key)) return "";
            return episode.getString(key);
        };

        while (morePages) {

            GetRequest request =
                    Unirest.get(endpoint + "/series/{seriesId}/episodes")
                            .routeParam("seriesId", "" + seriesId)
                            .header("accept", "application/json");
            request.queryString("page", "" + page);

            HttpResponse<JsonNode> jsonResponse =
                    request.asJson();

            if (jsonResponse.getStatus() != 200)
                throw new Exception(
                        String.format("HTTP Error: Status=%d Body='%s'",
                                jsonResponse.getStatus(),
                                jsonResponse.getBody().toString()));

            //Look at the links and see if there is another page

            try {
                JSONObject links = (JSONObject)jsonResponse.getBody().getObject().get("links");
                int thisPage=links.getInt("last");
                morePages=thisPage!=page;
            } catch (Exception e) {
                throw new Exception("Could not find pagination data");
            }

            JSONArray data = null;
            try {
                data = (JSONArray)jsonResponse.getBody().getObject().get("data");
                for (int x=0; x<data.length(); x++) {
                    JSONObject episode = (JSONObject)data.get(x);
                    Date firstAired = Date.from(Instant.ofEpochSecond(0));
                    try {
                        firstAired=formatter.parse(episode.getString("firstAired"));
                    } catch (Exception e){ /*unparseable date*/ }
                    try {
                        list.add(new TheTvDbEpisode(
                                safeGetLong.apply(episode,"seriesId"),
                                safeGetLong.apply(episode,"id"),
                                safeGetInt.apply(episode,"airedSeason"),
                                safeGetInt.apply(episode,"airedEpisodeNumber"),
                                safeGetString.apply(episode,"episodeName"),
                                firstAired,
                                safeGetString.apply(episode,"overview")
                        ));
                    } catch (Exception e) {
                        throw new Exception("List.add error: "+episode.toString(),e);
                    }
                }
            } catch (Exception e) {
                throw new Exception("JSON parsing error",e);
            }

            page++;
        }

        return list;
    }

    @Override
    public TheTvDbSeries getSeries(long seriesId) throws Exception {
        GetRequest request =
                Unirest.get(endpoint + "/series/{seriesId}")
                        .routeParam("seriesId", "" + seriesId)
                        .header("accept", "application/json");
        HttpResponse<JsonNode> jsonResponse =
                request.asJson();

        if (jsonResponse.getStatus() != 200)
            throw new Exception(
                    String.format("HTTP Error: Status=%d Body='%s'",
                            jsonResponse.getStatus(),
                            jsonResponse.getBody().toString()));


        try {
            JSONObject data = (JSONObject) jsonResponse.getBody().getObject().get("data");
            long id = data.getLong("id");
            String name = data.getString("seriesName");
            Matcher matcher = Pattern.compile("^(\\d\\d\\d\\d)-\\d\\d-\\d\\d$").matcher(data.getString("firstAired"));
            int year=1900;
            if (matcher.matches())
                year = Integer.parseInt(matcher.group(1));
            return new TheTvDbSeries(id, name,year);
        } catch (Exception e) {
            throw new Exception("JSON parsing error", e);
        }
    }
    /*
    {"apikey": "cd8ee67d586cbe321ead08195543e0bd",
  "username": "LOWERYT8Z",
  "userkey": "8VE95MRPJFIWTCMV"}
     */


    public TheTvDbApiV3(String dirpath) {

            String metaFilePath = dirpath + "\\meta.txt";

        BiFunction<Integer,Integer,String> episodeKey = (s,e) -> {
            return String.format("S%02dE%02d",s,e);
        };

        try {

            Map<String,String> map  = Util.parseMetaFile(metaFilePath);
            if (!map.containsKey("SeriesId") || !map.containsKey("Year"))
                throw new Exception(String.format("File %s: Must include THETVDB.COM SeriesId and Year",metaFilePath));

            TheTvDb tdb = new TheTvDbApiV3("cd8ee67d586cbe321ead08195543e0bd","LOWERYT8Z","8VE95MRPJFIWTCMV");

            long seriesId = Long.parseLong(map.get("SeriesId"));
            int year = Integer.parseInt(map.get("Year"));

            TheTvDbSeries series = tdb.getSeries(seriesId);

            String normalizedTheTvDbSeriesName = series.getNameWithYear();
            String normalizedMetaFileSeriesName = series.normalizeName(map.get("Series"),year);
            if (! normalizedTheTvDbSeriesName.equals(normalizedMetaFileSeriesName))
                throw new Exception(String.format("Series name mismatch for Series ID %d: Meta='%s'  THETVDB.COM='%s'",
                seriesId, normalizedTheTvDbSeriesName,normalizedMetaFileSeriesName));
            List<TheTvDbEpisode> list = tdb.getEpisodes(seriesId);

            final Map<String,TheTvDbEpisode> keyMap = new HashMap<>();
            list.stream().forEach(e -> keyMap.put(episodeKey.apply(e.getSeason(),e.getAiredOrder()),e));

            final Pattern pattern = Pattern.compile("^(.+)\\.(?:(?:S(\\d+)E(\\d+))|(?:(\\d+)x(\\d+))).*\\.(\\w+)$",Pattern.CASE_INSENSITIVE);
            final int Series= 1;
            final int Sa= 2;
            final int Sb= 4;
            final int Ea= 3;
            final int Eb= 5;
            final int Extension = 6;

            Files.list(new File(dirpath).toPath()).forEach(

                    path -> {
                        //See if the path conforms to am

                        Matcher matcher = pattern.matcher(path.getFileName().toString());
                        if (!matcher.matches())
                            System.err.println("Can't parse: "+path.toString());
                        else {
                            int season = matcher.group(Sa)==null?Integer.parseInt(matcher.group(Sb)):Integer.parseInt(matcher.group(Sa));
                            int episode = matcher.group(Ea)==null?Integer.parseInt(matcher.group(Eb)):Integer.parseInt(matcher.group(Ea));

                            String key = episodeKey.apply(season,episode);

                            TheTvDbEpisode ep = keyMap.get(key);
                            if (ep==null)
                                System.err.println("Can't find "+key);
                            else
                            {
                                String originalEpisodeName=ep.getName();
                                String encodedEpisodeName=Util.encodeIllegalFileCharacters(originalEpisodeName);
                                if (!encodedEpisodeName.equals(originalEpisodeName)) {
                                    String recodedEpisodeName = Util.decodeIllegalFileCharacters(encodedEpisodeName);
                                    if (!originalEpisodeName.equals(recodedEpisodeName))
                                        throw new Error("Encoding/decoding error on episode name");  //Not necessary after this has been thoroughly tested
                                }
                                String newName=String.format("%s - %s.%s",key,encodedEpisodeName,matcher.group(Extension));
                                Path newPath = path.getParent().resolve(newName);
                                System.out.println(String.format(
                                        "RENAME.........: %s\n" +
                                        "              to %s",path.toAbsolutePath(),
                                                              newPath.toAbsolutePath())
                                );
                                try {
                                    Files.move(path, newPath);
                                } catch (Exception e) {
                                    System.err.println("Could rename "+path+" to "+newPath);
                                }
                            }

                        }

                    }

            );


        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static void main(String ... args) {
        try {
            String directory =
            //"F:\\VPS\\Discovery"
            "F:\\VPS\\WandaVision"
            //"F:\\VPS\\The Expanse"
            //"F:\\VPS\\Pandora\\Season 2"
            ;

            new TheTvDbApiV3(directory);
            new UploadServer(directory);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
