package com.craiglowery.java.thetvdb;

import com.craiglowery.java.common.Util;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TheTvDbSeries {

    long id;
    String name;
    int year;

    /**
     * Creates an object representing {@code thetvdb.com} metadata for a TV series.
     * @param id The series ID.
     * @param name The name of the episode.
     */
    public TheTvDbSeries(long id, String name, int year) {
        this.id = id;
        this.name = name;
        this.year=year;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    private static Pattern namePattern = Pattern.compile("^\\s*(.+?)(?:\\s+\\((19\\d\\d|20\\d\\d)\\)\\s*)?$");

    /**
     * Takes a series name and year of broadcast and returns a normalized name.
     *
     *    Series names can be of the form  "Name (Year)" or "Name".
     *    A normalized name is always "Name (Year)".
     *    This function does the following:
     *    <ol>
     *        <li>Parse the name to determine baseName and yearInName</li>
     *        <li>If the year is null
     *            <ol>
     *                <li>If yearInName is null then there is no year information at all, so return
     *                the baseName with year 1900 as a default</li>
     *                <li>The name as provided has a year component and is all we have to go on,
     *                so return it.</li>
     *            </ol>
     *        </li>
     *        <li>Else (the year is NOT null)
     *           <ol>
     *               <li>If yearInName is null
     *                  <ol>
     *                      <li>Return the title with the year added to the end of it</li>
     *                  </ol>
     *               </li>
     *               <li>
     *                   Else (yearInname is not null)
     *                   <ol>
     *                      <li>If year==yearInName return original name</li>
     *                      <li>Else throw an exception (year and yearInName do not match)</li>
     *                   </ol>
     *               </li>
     *           </ol>
     *        </li>
     *    </ol>
     * @param name
     * @param year
     * @return
     */
    public static String normalizeName(String name, Integer year) throws Exception {
        String baseName = getNameFromNameString(name);
        Integer yearInName = getYearFromNameString(name);
        if (year==null) {
            if (yearInName==null)
                return String.format("%s (1900)",baseName);
            return name;
        } else /* year!=null */ {
            if (yearInName==null) {
                return String.format("%s (%d)",baseName,year);
            } else {
                if (year.equals(yearInName))
                    return name;
                throw new Exception(String.format("year!=yearInName  %d!=%d  - %s",year,yearInName,name));
            }
        }
    }

    public static String getNameFromNameString(String nameString) {
        Matcher matcher = namePattern.matcher(nameString);
        if (!matcher.matches())
            return nameString;
        return matcher.group(1);
    }

    public static Integer getYearFromNameString(String nameString) {
        Matcher matcher = namePattern.matcher(nameString);
        if (!matcher.matches())
            return null;
        if (matcher.group(2)==null)
            return null;
        return Integer.parseInt(matcher.group(2));
    }

    public String getNameWithoutYear() {
        return getNameFromNameString(name);
    }


    public String getNameWithYear() throws Exception {
        return normalizeName(name, year);
    }

    public int getYear() {
        return year;
    }

    @Override
    public boolean equals(Object other) {
        TheTvDbSeries otherSeries = (TheTvDbSeries)other;
        try {
            return getNameWithYear().equals(otherSeries.getNameWithYear());
        } catch (Exception e) {
            return false;
        }
    }

}
