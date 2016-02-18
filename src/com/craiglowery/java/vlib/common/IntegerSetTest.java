package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import static org.junit.Assert.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Stack;

import org.junit.Test;

/** 
 * Unit test for the bit-mapped integer set class.
 *
 */
public class IntegerSetTest {

	@Test
	public void basicTests() {
		IntegerSet s = new IntegerSet();
		
		//New sets should have empty range and no values
		assertEquals("New sets should have 0 size",s.size(),0);
		assertNull("New sets have null upper bound",s.getUpperBound());
		assertNull("New sets have null lower bound",s.getLowerBound());
		assertEquals("New sets have a 0 rangeSize",0,s.rangeSize());
		assertNull("New sets have null mininum",s.min());
		assertNull("New sets have null maximum",s.max());
		
		//The complement of the empty null-range set should be the empty null-range
		IntegerSet t = s.complement();
		assertEquals("Complements of new sets should have 0 size",t.size(),0);
		assertNull("Complements of new sets have null upper bound",t.getUpperBound());
		assertNull("Complements of new sets have null lower bound",t.getLowerBound());
		assertEquals("Complements of new sets have a 0 rangeSize",0,t.rangeSize());
		assertNull("Complements of new sets have null mininum",t.min());
		assertNull("Complements of new sets have null maximum",t.max());
		
		//Two empty null-range sets should be considered equal
		assertEquals(s,t);
		
		
		//Insert a single value out in the middle of the set
		s.add(83720);
		assertEquals(1,s.size());
		assertEquals(s.getUpperBound(),new Integer(83720));
		assertEquals(s.getLowerBound(),new Integer(83720));
		assertEquals(1,s.rangeSize());
		assertEquals(s.min(),new Integer(83720));
		assertEquals(s.max(),new Integer(83720));
		s.dump(System.out);
		assertArrayEquals(new Integer[] {83720},s.toArray());
		
		assertNotEquals(s,t);
		
		//The complement
		t = s.complement();
		assertEquals(0,t.size());
		assertEquals(1,t.rangeSize()); 
		assertArrayEquals(new Integer[] {}, t.toArray());
		
		IntegerSet u = t.complement();
		assertEquals(s,u);
		assertTrue(s.toString().equals(u.toString()));
		System.out.println(s.toString());
		System.out.println(u.toString());
		assertNotEquals(s.toString(),t.toString());
	}
	
	@Test
	public void StressTest() {
		for (int x = 0; x<Integer.MAX_VALUE; x++) {
			testLargeSetsOfLargeRange();
			testLargeSetsOfSmallRange();
			testMediumSetsOfLargeRange();
			testMediumSetsOfSmallRange();
			testSmallSetOfLargeRange();
			testSmallSetsOfSmallRange();
		}
	}
	
	private boolean shorttest=true;
	
	@Test
	public void testSmallSetsOfSmallRange() {
		System.out.println("TEST: Small sets of small range numbers");
		randomTest(500,1000,0,1000);
	}
	
	@Test
	public void testSmallSetOfLargeRange() {		
		System.out.println("TEST: Small sets of large range of numbers");
		randomTest(500,1000,0,Integer.MAX_VALUE-1);
	}

	@Test
	public void testMediumSetsOfSmallRange() {
		System.out.println("TEST: Medium sets of small range numbers");
		randomTest(200000,500000,0,9000000);
	}
	
	@Test
	public void testMediumSetsOfLargeRange() {		
		System.out.println("TEST: Medium sets of large range of numbers");
		randomTest(200000,500000,0,Integer.MAX_VALUE-1);
	}

	@Test
	public void testLargeSetsOfSmallRange() {
		System.out.println("TEST: Large sets of small range numbers");
		randomTest(200000,500000,0,9000000);
	}
		
	@Test
	public void testLargeSetsOfLargeRange() {
		System.out.println("TEST: Large sets of large range of numbers");
		randomTest(200000,500000,0,Integer.MAX_VALUE-1);

	}

	Random ran = new Random();
	Stack<Instant> times = new Stack<Instant>(); 
	private void mark() {
		times.push(Instant.now());
	}
	private String duration() {
		return Duration.between(times.get(times.size()-2),times.get(times.size()-1)).toString();
	}


	private void randomTest(int setSizeRangeLow, int setSizeRangeHigh, int setLowerValue, int setHigherValue) {
		System.out.println(String.format("Set size range %d..%d   Value range %d..%d",setSizeRangeLow,setSizeRangeHigh,setLowerValue,setHigherValue));
		int someSize = setSizeRangeLow+ran.nextInt(setSizeRangeHigh-setSizeRangeLow);
		
		mark();
		Set<Integer> set = new HashSet<Integer>(someSize);
		for (int x=0; x<someSize; x++)
			while (!set.add(setLowerValue+ran.nextInt(setHigherValue-setLowerValue))) {}
		mark();
		System.out.println(String.format("  Time to create source Set<> of %d values: %s",someSize,duration()));

		mark();
		Set<Integer> set2 = new HashSet<Integer>(someSize);
		for (Integer i : set) {
			set2.add(i);
		}
		mark();
		System.out.println(String.format("  Time to create comparison Set<> of %d values: %s",someSize,duration()));
		set=null;
		
		IntegerSet u = new IntegerSet();
		mark();
		for (Integer i : set2) {
			u.add(i);
		}
		mark();
		assertEquals(set2.size(),u.size());
		System.out.println(String.format("  Time to create IntegerSet of %d values: %s",someSize,duration()));

		System.out.println("   Testing equality...");
		testEquality(set2,u);
		System.out.println("      ...done");
		
		System.out.println("   Random agreement test in progress...");
		int NUM_COMPARES=1000000;
		for (int x=0; x<NUM_COMPARES; x++) {
			int i=ran.nextInt(Integer.MAX_VALUE-1);
			assertEquals(set2.contains(i),u.contains(i));
		}
		System.out.println("      ...done");
		
		for (int b = 50; b>0 ; b<<=2) {
			System.out.println(String.format("  Comparing membership test times for %d tests",b));
			mark();
			for (int x=0; x<b; x++) {
				set2.contains(ran.nextInt(Integer.MAX_VALUE-1));
			}
			mark();
			System.out.println(String.format("       java.util.Set<Integer> took %s",duration()));
			
			mark();
			for (int x=0; x<b; x++) {
				u.contains(ran.nextInt(Integer.MAX_VALUE-1));
			}
			mark();
			System.out.println(String.format("       IntegerSet took............ %s",duration()));
			if (shorttest)
				break;
		}

		IntegerSet savedu = u.clone();
		IntegerSet removedFromu = new IntegerSet();
		Set<Integer> saveuset = new HashSet<Integer>();
		saveuset.addAll(set2);
		Set<Integer> removedFromset2 = new HashSet<Integer>();
		
		System.out.println("Removal test");
		//Now let's remove up to half the set
		int z=someSize/2;
		int v;
		int removed=0;
		for (int x=0; x<z; x++) {
			v= setLowerValue+ran.nextInt(setHigherValue-setLowerValue);
			boolean ucont = u.contains(v);
			boolean scont = set2.contains(v);
			if (ucont!=scont) {
				assertTrue(ucont==scont);
			}
			boolean urem = u.remove(v);
			boolean srem = set2.remove(v);
			if (urem!=srem) {
				assertTrue(urem==srem);
			}
			if (urem) {
				removed++;
				removedFromu.add(v);
				removedFromset2.add(v);
			}
		}
		
		System.out.println(String.format("%d elements removed",removed));
		
		
		System.out.println("Testing equality");
		testEquality(set2,u);
		
		System.out.println("Testing complementation");
		IntegerSet uu = u.complement();
		System.out.println("  Size of original set....: "+u.size());
		System.out.println("  Range size of original..: "+u.rangeSize());
		System.out.println("  Size of complemented set: "+uu.size());
		int k=u.rangeSize()-u.size();
		System.out.println("  Size of comp should be..: "+k);
		System.out.println("  The difference is.......: "+(k-uu.size()));
		//System.out.println("  Range size of complement: "+uu.rangeSize());
		assertEquals(u.rangeSize()-u.size(),uu.size());
		
		System.out.println("\nTesting clone");
		IntegerSet un = u.clone();
		assertEquals(un,u);
		
		un=u.union(uu);
		System.out.println("  Size of union result....: "+un.size());
		System.out.println("  Range size of union.....: "+un.rangeSize());
		assertEquals(un.size(),un.rangeSize());
		assertEquals(un.min().intValue(),un.lowerBound);
		assertEquals(un.max().intValue(),un.upperBound);
		
		System.out.println("Testing intersection");
		IntegerSet inter = uu.intersect(u);
		assertEquals(inter.size(),0);
		uu=null;
		
		System.out.println("Testing difference");
		IntegerSet diff = savedu.difference(removedFromu);
		savedu=removedFromu=null;
		assertEquals(u,diff);
		saveuset.removeAll(removedFromset2);
		testEquality(saveuset,diff);
		
		System.out.println("---PASSED---\n");
	}

	private void testEquality(Set<Integer> set, IntegerSet iset) {
		int min=Integer.MAX_VALUE;
		int max=Integer.MIN_VALUE;
		for (Integer i : set) {
			assertTrue("Value in Set<> not in IntegerSet: "+i,iset.contains(i)) ;
		}
		int last=Integer.MIN_VALUE;
		Iterator<Integer> it = iset.iterator();

		assertEquals("size compared to Set<>",iset.size(),set.size());

		int i;
		int card=0;
		while (it.hasNext()) {
			card++;
			i=it.next();
			if (i<min) min=i;
			if (i>max) max=i;
			assertTrue(String.format("Values not returned in ascending order %d ... %d",last,i),i>last);
			last=i;
			assertTrue("Value in IntegerSet not in Set<>"+i,set.contains(i));
		}
		assertEquals("Min",iset.min().intValue(),min);
		assertEquals("Max",iset.max().intValue(),max);
		assertEquals("size based on iteration",iset.size(),card);

	}
	
}
