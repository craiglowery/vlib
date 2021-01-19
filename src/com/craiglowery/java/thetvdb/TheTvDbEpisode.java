package com.craiglowery.java.thetvdb;

import java.util.Date;

public class TheTvDbEpisode {

    long seriesId;
    long id;
    int season;
    int airedOrder;
    String name;
    Date airDate;
    String overview;

    /**
     * Creates an object representing {@code thetvdb.com} metadata for a TV series episode.
     * @param seriesId The series ID.
     * @param id The episode ID.
     * @param season The season ID.
     * @param airedOrder The aired order (sequence).
     * @param name The name of the episode.
     * @param airDate The first-aired date.
     * @param overview An overview of the episode (plot).
     */
    public TheTvDbEpisode(long seriesId, long id, int season, int airedOrder, String name, Date airDate, String overview) {
        this.seriesId = seriesId;
        this.id = id;
        this.season = season;
        this.airedOrder = airedOrder;
        this.name = name;
        this.airDate = airDate;
        this.overview = overview;
    }

    public long getSeriesId() {
        return seriesId;
    }

    public long getId() {
        return id;
    }

    public int getSeason() {
        return season;
    }

    public int getAiredOrder() {
        return airedOrder;
    }

    public String getName() {
        return name;
    }

    public Date getAirDate() {
        return airDate;
    }

    public String getOverview() {
        return overview;
    }
}
