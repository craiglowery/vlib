package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.tuple.Attribute;
import com.craiglowery.java.vlib.tuple.PrimaryKey;
import com.craiglowery.java.vlib.tuple.Tuple;
/**
 * This is a testing Tuple.
 *
 */
public class Test {
	
	
	public class SimpleTuple extends Tuple  {
		@PrimaryKey 	int 	handle;
		@PrimaryKey 	Instant imported;
		@Attribute 		long 	length;
		@Attribute 		String 	sha1sum;
		@Attribute  	String 	title;
		@Attribute 		String 	path;
		@Attribute 		String 	copiedfrom;
		@Attribute		long	inode;
		@Attribute		Instant	hm_lastseen;
		@Attribute		Instant hm_lastfingerprinted;
		@Attribute		boolean	hm_unhealthy;
		@Attribute		boolean	hm_lengthmismatch;
		@Attribute		long	hm_linkcount;
		@Attribute		Instant	hm_lastsuccessfulvalidation;
		@Attribute		Instant	hm_lastvalidationattempt;
		@Attribute		boolean	hm_corrupt;
		@Attribute		String	hm_message;
		@Attribute		boolean	hm_healthchanged;
		@Attribute		String	hm_lastobservedchanges;
		@Attribute		int		hm_versioncount;
	
		public SimpleTuple() throws U_Exception { 
			super();
		}
		
	}
	
	
	public static void main(String[] args) {

		
				
		
		try {
			TupleExpressionFactory f = new TupleExpressionFactory(SimpleTuple.class);
			InputStreamReader isr = new InputStreamReader(System.in);
			java.io.BufferedReader b = new BufferedReader(isr);
			
			String s;
			while ( true) {
				System.out.print(">");
				System.out.flush();
				s = b.readLine();
				if (s==null) break;

				StringExpressionParser p =null;
				TupleExpressionFactory.Node exp=null;
				List<String> errorList = null;
				try {
					errorList = new LinkedList<String>();
					p = new StringExpressionParser(s,f,errorList);

					exp = (TupleExpressionFactory.Node)(p.parse().value);
					exp.treeView(System.out);
					System.out.println();
				} catch (Exception e) {
					for (String ss : errorList)
						System.err.println(ss);
					if(errorList.size()==0) System.err.println(e.getMessage());
					System.err.flush();
					continue;
				}
				try {
					String expanded = PostgresqlExpressionGenerator.expand(exp);
					System.out.println(expanded);
				} catch (Exception e) {
					System.err.println("Could not postgres expand: "+e.getMessage());
				}				
			} 
		} catch (FilterExpressionException e) {
			System.err.println("Could not create the factory: "+e.getMessage());
		} catch (IOException e) {
			System.err.println("Error reading input: "+e.getMessage());
		}
			

		
		
			
		
	}

}
