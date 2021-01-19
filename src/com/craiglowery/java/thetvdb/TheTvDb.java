package com.craiglowery.java.thetvdb;

import java.util.Iterator;
import java.util.List;

public interface TheTvDb {

    List<TheTvDbEpisode> getEpisodes(long seriesId) throws Exception;
    TheTvDbSeries getSeries(long seriesId) throws Exception;
}
