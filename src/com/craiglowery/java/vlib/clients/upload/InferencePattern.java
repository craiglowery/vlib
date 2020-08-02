package com.craiglowery.java.vlib.clients.upload;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by craig on 2/24/2017.
 *
 * A single inference pattern, which is a regular expression plus a mapping of capture groups to
 * attribute names.
 *
 * The map is keyed on the group number integer and returns a string which is the attribute to be
 * mapped to the captured value.
 */
public class InferencePattern {

    Pattern pattern;
    Map<Integer,String> map;
    String description;

    /**
     * Create a new inference pattern.
     * @param re  The regular expression, which should include capture groups.
     * @param map Map keyed on integers which link capture groups to the attribute name, which is the string value of the
     *            mapping.
     */
    public InferencePattern(String description, String re, Map<Integer,String> map) {
        pattern = Pattern.compile(re);
        this.description = description;
        this.map=map;
    }

    private static boolean isOdd(int x) {
        return (x%2)!=0;
    }

    public static Map<Integer,String> makeMap(Object...args) {
        if (isOdd(args.length))
            throw new RuntimeException("makeMap argument list must be an even length");
        Map<Integer,String> map = new HashMap<>();
        for (int x=0; x<args.length; x+=2) {
            if (!args[x].getClass().isAssignableFrom(Integer.class) ||
                !args[x+1].getClass().isAssignableFrom(String.class))
                throw new RuntimeException("makeMap arguments must be Integer String ...");
            map.put((Integer)(args[x]),(String)(args[x+1]));
        }
        return map;
    }

    /**
     * Matches the string {@code s} against the pattern.  If any capture groups are non-null, then they are placed
     * in the return map which is keyed on attribute name associted with the capture group.
     * @param s  The string to match against
     * @return The mapping of attribute names to string values.  Only attributes for which the capture group is non-null
     * will be included.  No match will return {@code null}.
     */
    public Map<String,String> match(String s) {
        Matcher m = pattern.matcher(s);
        if (!m.matches())
            return null;
        int groups  = m.groupCount();
        Map<String,String> rval = new HashMap<String,String>();

        for (int group=1; group<=groups; group++) {
            if (map.containsKey(group))
                rval.put(map.get(group),m.group(group));
        }

        return rval;
    }

    @Override
    public String toString() {
        return description;
    }

}
