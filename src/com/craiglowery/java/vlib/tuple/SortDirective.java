package com.craiglowery.java.vlib.tuple;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;

public class SortDirective {
	
	public String attribute;
	
	public enum Order {Ascending, Descending};
	
	public Order order;
	
	public SortDirective(String att, Order ord) { attribute=att; order=ord; }

	private static Pattern PATTERN_SORT_DIRECTIVE = null;

	/**
	 * <p>Builds a new {@code SortDirective[]} based on a SQL-like {@code ORDER BY} expression.
	 * The expression should be a list of comma separated directives of the form
	 * <it>attributeName</it>&nbsp;ASC|DESC.</p>
	 * 
	 * <p>{@code U_Exception} error codes thrown directly from this method:</p>
	 * <ul>
	 *   <li><b>Unexpected</b> - if the regular expression fails to compile
	 *   <li><b>BadParameter</b> - malformed {@code orderby} expression.
	 * </ul>
	 * @param orderby The order by expression, such as {@code name asc, age desc}.
	 */
	public static SortDirective[] build(String orderby) throws U_Exception {
		if (orderby==null || (orderby=orderby.trim()).equals(""))
			return null;
		String[] directives = orderby.split(",");
		Object args[] = new Object[2*directives.length];
		if (PATTERN_SORT_DIRECTIVE==null) {
			try {
				PATTERN_SORT_DIRECTIVE = Pattern.compile("^\\s*([A-Z_$][A-Z0-9_$]*)\\s+(?:(ASC|DESC)(?:ENDING)?)\\s*$",Pattern.CASE_INSENSITIVE);
			} catch (PatternSyntaxException e) {
				throw new U_Exception(ERROR.Unexpected,"compiling sort directive expression RE",e);
			}
		}
		int argx=0;
		for (String directive : directives) {
			Matcher m = PATTERN_SORT_DIRECTIVE.matcher(directive);
			if (!m.matches())
				throw new U_Exception(ERROR.BadParameter,"invalid sort directive: "+directive);
			args[argx++]=m.group(1);
			args[argx++]= m.group(2).toUpperCase().equals("DESC")?SortDirective.Order.Descending:SortDirective.Order.Ascending;
		}
		return build(args);
	}
	
	/**
	 * <p>Builds a new SortDirective[] based on a list of arguments.</p>
	 * <p>{@code U_Exception} error codes thrown directly from this method:</p>
	 *  <ul>
	 *   <li><b>Unexpected</b> - if the regular expression fails to compile
	 *   <li><b>BadParameter</b> - the number of args is not even.
	 * </ul>
	 * @param args An even-number list of objects, or an Object[] array.  The even numbered positions (0,2,4..) are
	 *    of type String and represent attribute names to sort by.  The odd numbered positions (1,3,5..) are
	 *    Order enum values (Ascending, Descending) that define the sort order of the attribute immediately 
	 *    preceding it in the list.
	 * @return An array that embodies the provided arguments.
	 * @throws U_Exception
	 */
	public static SortDirective[] build(Object...args) throws U_Exception {
		if (args.length % 2 != 0) throw new U_Exception(U_Exception.ERROR.BadParameter,"Badly formed sort directive args (number)");
		List<SortDirective> l = new LinkedList<SortDirective>();
		for (int x=0; x<args.length; x+=2) {
			if (args[x].getClass()!=String.class || args[x+1].getClass()!=Order.class) throw new U_Exception(U_Exception.ERROR.Unexpected,"Badly formed sort directive args (type)");
			l.add(new SortDirective((String)args[x],(Order)args[x+1]));
		}
		return l.toArray(new SortDirective[0]);
	}
	
	public final static SortDirective[] NONE = new SortDirective[0];
	
}