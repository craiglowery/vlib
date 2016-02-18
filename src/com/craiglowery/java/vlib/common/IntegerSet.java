package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;



/**
 * Implements a high-performance bit-mapped set of non-negative integers, supporting
 * set operations such as intersection, union, complementation and difference.<p>
 *
 * The domain of any set that is an instance of this is potentially that of
 * the integers 0 to {@code Integer.MAX_VALUE-1}. The &quot;actual&quot; domain of any
 * instance is defined by a lower and upper bound, which we refer to as the
 * <i>range</i> of that instance. This is necessary to support
 * the complement operation, because it is unlikley that all applications for the use
 * of this class will actually range over the universal domain {@code 0..Integer.MAX_VALUE-1}.  
 * Think of the range of an instance as itself being a set that
 * includes all integers from the lower bound to the upper bound, inclusive, that
 * are logically in that instance's domain.  Any complementation operation will not consider
 * integer values outside of that range.<p>
 * 
 * A set may be configured to have an auto-ranging or fixed-range domain.<p>
 * 
 * If auto-ranging, then the range expands to accommodate the
 * union of ranges participating in an operation.  For example, if set A has range
 * 5..19 and set B has range 0..12, then any operation that involves both A and B
 * will result in a set with range 5..19.  Inserting new members, such as adding the number
 * 20 to set A, is the same as unioning set A with the singleton set (20), resulting in
 * A's new range being 5..20.  Auto-ranging sets initially have no range (the empty set).<p>
 * 
 * If fixed-range, then the range must be declared upon construction of the instance, and 
 * any operations performed with the instance must be with other fixed-range sets of the
 * same range.<p>
 * 
 * Fixed-range sets can be made auto-ranging, and vice-versa, using the {@code autoRanging}
 * method.<p>
 * 
 * Any set's range can be arbitrarily set by using the {@code setRange} method, with the
 * caveat that the proposed new range must include all members of the set.<p>
 * 
 * The implementation's performance varies depending upon the range of values in a set's domain
 * (it's range), the size of the set (how many values are in it), and the types of operations
 * performed. 
 * 
 * 
 * @author James Craig Lowery
 *
 */
public class IntegerSet implements Iterable<Integer>, Set<Integer>, Collection<Integer> {
	
	/*
	 * IMPLEMENTATION NOTES
	 * 
	 * We conceptually implement a sparse bit-array where a member is denoted as being "in" the
	 * set when the corresponding bit in the array is 1.  Sparseness of the array is achieved
	 * by combination of a multi-dimensional hash set of arrays of 64-bit (long) integers.
	 * 
	 * The maxmimum value (Integer.MAX_VALUE-1 in java) is 2^31-1, or 2147483646, requiring 31 bits.  
	 * This class creates sets that must be able to represent any and all values in this range, 
	 * which will require 2^25 (33,554,432) long integers considered conceptually as a single array.  
	 * The ability to allocate, quickly find, and iterate over these 64-bit blocks is at the core 
	 * of the problem this class solves.
	 * 
	 * Rather than optimize for a uniform distribution across the entire domain, we optimize for ranges that
	 * tend to cluster around much smaller sub-ranges as it suits the application for which this 
	 * class was originally developed. (A uniform distribution would be better served by another 
	 * implementation.)To achieve this, we adopt an extent mechanism where an extent embodies a "likely"
	 * sized cluster, which we have arbitrarily set at 2^15 (32,768) values, which requires 2^9 (512) 
	 * 64-bit bitmaps. The entire domain would require 2^16 (65,536) extents to be fully populated.
	 * 
	 * For any given value in the domain, we must know its extent number (EXTENT), its position within
	 * the extent (CHUNK) which is the index in the long[512] array, and then the bit position within that
	 * chunk (BIT).
	 * 
	 * As a reminder, here are the ranges for these index values:
	 * 
	 *    EXTENT:  0..65,535
	 *    CHUNK:   0..511
	 *    BIT:     0..63
	 *    
	 * An extent is represented by the inner class Extent, which has an extent number (its position in the range
	 * 0..65.535) and the long[512] array.  Extents are hashed in a TreeSet<Extent> set, which uses the position
	 * number to guarantee that the extents can be iterated over in ascending order.  This facilitates efficient
	 * implementation of the toArray() method, as well as guarantees log(n) access to any extent given its EXTENT
	 * number.  The Extent class also provides a static method for computing an EXTENT number based on a member
	 * value.  It also knows how to compute the CHUNK and BIT indexes for a value.  The mapping is as follows:
	 * 
	 *    
	 *        _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _   - a value requires 31 bits to represent
	 *        3 2 2 2 2 2 2 2 2 2 2 1 1 1 1 1 1 1 1 1 1
	 *        0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0 9 8 7 6 5 4 3 2 1 0
	 *        _______________________________ _________________ ___________
	 *                 EXTENT#                     CHUNK#          BIT#
	 *                 
	 * So, the bit operation to retrieve the EXTENT from a value is to shift the value right (zero-filled) 16 places
	 * 
	 *                           VALUE >>> 16
	 *                           
	 * The bit operation to retrieve the CHUNK from a value is to shift the lower 16 bits right 6 places
	 * 
	 *                           (VALUE & 0x7FFF) >>> 6
	 *                           
	 * The bit operation to retrieve the BIT from a value is to simply mask the lower 6 bits
	 * 
	 *                          VALUE & 0x3F
	 */
	
	static final boolean paranoid = true;   //If true, extra code that does sanity checks is compiled. Good for testing! Turn off for performance.
	
	static final int INTEGER_SIZE = Integer.SIZE;
	static final int VALUE_WIDTH = 31;    
	static final int EXTENT_INDEX_FIELD_WIDTH = 16;
	static final int CHUNK_INDEX_FIELD_WIDTH = 9;
	static final int BIT_INDEX_FIELD_WIDTH = 6;
	
	static final int BIT_INDEX_FIELD_LSB = 0;
	static final int CHUNK_INDEX_FIELD_LSB = BIT_INDEX_FIELD_LSB + BIT_INDEX_FIELD_WIDTH;
	static final int EXTENT_INDEX_FIELD_LSB = CHUNK_INDEX_FIELD_LSB + CHUNK_INDEX_FIELD_WIDTH;
	
	static final int BIT_INDEX_FIELD_MSB = CHUNK_INDEX_FIELD_LSB-1;
	static final int CHUNK_INDEX_FIELD_MSB = EXTENT_INDEX_FIELD_LSB-1;
	static final int EXTENT_INDEX_FIELD_MSB = CHUNK_INDEX_FIELD_MSB + EXTENT_INDEX_FIELD_WIDTH;
	
	/** The number of extents possible in a set.  Extent numbers are in this range 1...NUMBER_OF_EXTENTS-1 */
	static final int EXTENTS_PER_SET = 1<<EXTENT_INDEX_FIELD_WIDTH;
	
	/** The number of chunks in an extent's chunk array. */
	static final int CHUNKS_PER_EXTENT  = 1<<CHUNK_INDEX_FIELD_WIDTH;
	
	/** The number of bits in a single chunk.  For a long, this should be 64 bits. */
	static final int BITS_PER_CHUNK    = 1<<BIT_INDEX_FIELD_WIDTH;
	
	/** Used to isolate the index of a value within an extent */
	static final int OFFSET_IN_EXTENT_MASK  = (-1 ^ (-1 << (CHUNK_INDEX_FIELD_WIDTH+BIT_INDEX_FIELD_WIDTH)));

	/** Used to isolate the index of a value within a chunk */
	static final int OFFSET_IN_CHUNK_MASK   = -1 >>> (EXTENT_INDEX_FIELD_WIDTH+CHUNK_INDEX_FIELD_WIDTH+32-VALUE_WIDTH);
	
	/** The number of set values that an extent can hold. */
	static final int VALUES_PER_EXTENT =   1 << (VALUE_WIDTH - EXTENT_INDEX_FIELD_WIDTH );
	
	/** The number of set values a chunk can hold */
	static final int VALUES_PER_CHUNK =   1 << (VALUE_WIDTH - EXTENT_INDEX_FIELD_WIDTH - CHUNK_INDEX_FIELD_WIDTH); //Better be 1<<5==64!!!
	
	public final static int LOWEST_BOUND = 0;
	public final static int HIGHEST_BOUND = Integer.MAX_VALUE-1;

	/** The total number of values that can be in a set */
	static final int VALUES_PER_SET = HIGHEST_BOUND-LOWEST_BOUND+1;

	/** Represents that a min or max is not known */
	private final static int UNKNOWN = -1;
	

	
	/** The lower bound of this set's range.  If it is greater-than the upper bound,
	 * then the range is empty.
	 */
	int lowerBound;
	
	/** The upper bound of this set's range. If it is less-than the lower bound,
	 * then the range is empty.
	 */
	int upperBound;
	
	/**
	 * Defines the current ranging behavior of the set.  If true, the upper and lower bounds
	 * float as needed to accommodate an never-contracting domain of non-negative integers.
	 */
	boolean autoRange;
	
	/**
	 * The tree set of extents.
	 */
	TreeSet<Extent> extentSet;
	
	/**
	 * Used to track existence of extents, because TreeSet has no easy way to
	 * determine set membership by key.
	 */
	Map<Integer,Extent> extentMap;
	
	/** The minimum value in this set if it is known */
	private int minSetValue;
	
	/** The maximum value in this set if it is known */
	private int maxSetValue;
	
	/** The cardinality of the set, if it is known */
	private Integer setCardinality;
	
	private enum EXTENT_CLONE_FUNCTION {IDENTITY, COMPLEMENT};
	
//------------------------------------------------------------------------------------------	
//-- Constructors 	
//------------------------------------------------------------------------------------------
	
	/**
	 * Create the default empty, auto-ranging instance.
	 */
	public IntegerSet() {
		super();
		clear();
	}
	
	/**
	 * Create an empty set with a fixed range.
	 * 
	 * @param lowerBound The lower bound of the fixed range. Must be non-negative and less than or equal to the upper bound.
	 * @param upperBound The upper bound of the fixed range. Must be non-negative and greater than or equal to the upper bound.
	 */
	public IntegerSet(int lowerBound, int upperBound) {
		this();
		if (!validBound(lowerBound) || !validBound(upperBound) || lowerBound>upperBound)
			throw new RuntimeException("Invalid range");
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		autoRange=false;
	}
	
	/**
	 * Creates a new, empty set that is range-compatible with the 
	 * other set, having the same upper and lower bounds (range)
	 * and auto-range setting.
	 * 
	 * @param other  The set after which this one is to be patterned.
	 */
	public IntegerSet(IntegerSet other) {
		this();
		this.lowerBound = other.lowerBound;
		this.upperBound = other.upperBound;
		this.autoRange = other.autoRange;
	}
	
	/**
	 * Creates a new set using Integer compatible objects drawn from a collection.
	 * 
	 * @param c A collection of objects that must be Integer or subclass of Integer.
	 */
	public IntegerSet(Collection<?> c) {
		this();
		for (Object i : c)
			add((Integer)i);
	}

//------------------------------------------------------------------------------------------	
//-- Public Methods 	
//------------------------------------------------------------------------------------------
	
//======Object overrides
	
	/**
	 * Creates a new set that is an exact copy of this one.
	 */
	@Override
	public IntegerSet clone() {
		IntegerSet result = new IntegerSet(this);
		result.minSetValue = minSetValue;
		result.maxSetValue = maxSetValue;
		result.setCardinality = setCardinality;
		for (Extent extent : extentSet) {
			result.addExtent(result.new Extent(extent,EXTENT_CLONE_FUNCTION.IDENTITY));
		}
		return result;
	}	
	
	/**
	 * Returns an integer value that is guaranteed to be the same as that of any other
	 * {@code IntegerSet} that is equal to this one.
	 * 
	 * @return A hash code value for this object.
	 */
	@Override
	public int hashCode() {
		//Things that affect set equality
		//   lowerBound     
		//   upperBound
		//   autoRange
		//   the specific set of members
		int hash = 0xF89A2309;
		if (lowerBound<=upperBound) {
			hash ^= lowerBound;
			hash ^= ~upperBound;
			if (autoRange) hash = ~hash;
			Iterator<Integer> it = iterator();
			while (it.hasNext())
				hash ^= it.next();
		} 
		return hash;
	}
	
	@Override
	public boolean equals(Object otherObj) {
		IntegerSet other = (IntegerSet)otherObj;
		if (rangeSize()==0 && other.rangeSize()==0)
			return true;
		if (lowerBound!=other.lowerBound ||
			upperBound!=other.upperBound ||
			autoRange!=other.autoRange ||
			min().intValue()!=other.min() ||
			max().intValue()!=other.max() ||
			size() !=other.size())
			return false;
		Iterator<Integer> a = iterator();
		Iterator<Integer> b = iterator();
		while (a.hasNext()) {
			if (paranoid && !b.hasNext()) 
				throw new RuntimeException("PARANOID: Inconsistent set state detected during equality test");
			if (a.next().intValue()!=b.next().intValue())
				return false;
		}
		return true;
	}
	
	
	/**
	 * Returns a string representation of this set, which always includes the range and
	 * cardinality, and up to the first 100 members of the set in the following format:<p>
	 * 
	 * <pre>
	 *              <i>hashcode-in-hex</i><b>:</b> <i>cardinality</i> <b>of</b> <i>lower</i><b>..</b><i>upper</i> <b>[</b><i>member</i><b> </b><i>member</i><b>,...]</b>
	 * </pre>
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(hextostring(hashCode())).append(": ").append(size()).append(" of ");
		if (lowerBound>upperBound)
			sb.append("(empty range)");
		else
			sb.append(lowerBound).append("..").append(upperBound);
		sb.append(" [");
		int hundred=100;
		Iterator<Integer> it = iterator();
		while (it.hasNext() && (hundred--)>0) {
			if (hundred<99)
				sb.append(" ");
			sb.append(it.next());
		}
		if (it.hasNext())
			sb.append("...");
		sb.append("]");
		return sb.toString();
	}
	
//======Set<Integer> contractual fulfillment
	
	/**
	 * Adds an integer to the set, if it does not already exist.
	 * @param i The integer value to be added. It must be in the range
	 * 0..Integer.MAX_VALUE-1.
	 * @return True if the set was changed.
	 */
	@Override
	public boolean add(Integer i) {
		return add(new Integer[] {i})!=0;
	}

	/**
	 * Adds Integer-compatible objects from a collection into this set.
	 * @param c The collection of Integer objects, or subclass of Integer.
	 * @return True if the set was changed.
	 */
	@Override
	public boolean addAll(Collection<? extends Integer> c) {
		boolean changed=false;
		for (Object o : c)
			changed |= add((Integer)o);
		return changed;
	}
	
	/**
	 * Removes a value from this set if it is a member, otherwise
	 * there is no effect.  
	 * 
	 * @param value The value to remove from this set.
	 * 
	 * @return True if the set was changed.
	 */
	public boolean remove(Object objValue) {
		/*
		 *  If the set is empty, return immediately.
		 *  Get the extent number for this value
		 *  Does the extent exist?  If not, return
		 *  Remove value from extent
		 *  If this really caused a delete, update cardinality, min, and max
		 *  If the extent is now empty, remove it
		 */
		int value=(Integer)objValue;
		if ( size()!=0) {
			int extentNumber = computeExtent(value);
			Extent extent = extentMap.get(extentNumber);
			if (extent!=null &&  /*5*/ extent.remove(value)) {
				setCardinality--;
				if (setCardinality.intValue()==0) {                    // If no more values, min and max go to UNKNOWN
					minSetValue=maxSetValue=UNKNOWN;
				} else {
					//Otherwise, if we just removed the min or max, that value is now considered UNKNOWN
					if (minSetValue==value) minSetValue=UNKNOWN;
					if (maxSetValue==value) maxSetValue=UNKNOWN;
				}
				/*7*/
				if (extent.extentCardinality.intValue()==0) {
					removeExtent(extent);
				}
				return true;
			}
		}
		return false;
	}
	
	/**
	 * For each Integer-compatible object in a collection, remove that value from
	 * the set if it is a member.
	 * @param c A collection of {@code Integer} or {@code Integer}-subclass objects.
	 * @return True if the set was changed.
	 */
	@Override
	public boolean removeAll(Collection<?> c) {
		boolean changed=false;
		for (Object o:c)
			changed |= remove((Integer)o);
		return changed;
	}
	
	/**
	 * Determines if a particular value is a member
	 * of this set.
	 * @param value The value in question.
	 * @return Returns true if value is in the set.
	 */
	public boolean contains(Object objValue) {
		/* 1. Is the set non-empty and the value in the set's range?
		 * 2. Compute the extent number
		 * 3. Does the extent exist?
		 * 4. Does the extent show the value as existent? 
		 */
		int value=(Integer)objValue; 
		if (size()==0 || value < lowerBound || value > upperBound)  //Could use min()/max(), but with trade-off overhead
			return false;
		/*2*/
		int extentNumber = computeExtent(value);
		/*3*/
		Extent extent = extentMap.get(extentNumber);
		if (extent==null) 
			return false;
		/*4*/
		return extent.isSet(value);
	}
	
	public boolean containsAll(Collection<?> c) {
		for (Object o:c)
			if (!contains(o))
				return false;
		return true;
	}
	
	/**
	 * Returns an array of the current members of the
	 * set.  The size of the array will be the same
	 * as the {@code size()} of the set and will
	 * be sorted in ascending order.  Calling this
	 * method on sets with very large cardinality
	 * is discouraged.  The exact threshold where
	 * it becomes problematic is installation
	 * dependent.  For very large sets, use the
	 * {@code Iterator} interface.
	 * 
	 * @return An array of the set's members.
	 */
	@Override
	public Integer[] toArray() {
		//Iterate in ascending order across the tree
		Integer[] list = new Integer[size()];
		int i=0;
		Iterator<Integer> it = iterator();
		while (it.hasNext()) 
			list[i++]=it.next();
		//Take the opportunity to refresh min/max values in case they are UNKNOWN
		if (list.length==0)
			minSetValue=maxSetValue=UNKNOWN;
		else {
			minSetValue=list[0];
			maxSetValue=list[setCardinality-1];
		}
		return list;
	}
	
	/**
	 * Populates an array with values of this set.  The array that is passed in
	 * is used if it is large enough.  If it is larger than necessary, the values
	 * at the end of the array past the last value are set to null.  If the
	 * array is too small, then a new array of the same type that is exactly large 
	 * enough to hold the result is created.
	 * @param a An array of type {@code Integer[]} that can be overwritten and used
	 * to return the results.
	 * @return The array {@code a} populated with the values of this set and padded
	 * at the end with {@code null} values if necessary, or a new set {@code Integer[]}
	 * set that is exactly large enough to hold the result.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) {
		if (a==null)
			throw new NullPointerException();
		if (!a.getClass().isAssignableFrom(Integer.class))
			throw new ArrayStoreException();
		int arrSize = a.length;
		int size = size();
		if (arrSize<size) {
			a= (T[])(new Object[size]);
			arrSize=size;
		}
		int x=0;
		for (Integer i : this)
			a[x++]=(T)i;
		while (x<arrSize)
			a[x++]=null;
		return a;
	}
	

	/**
	 * For each object in a collection, remove that value from the set if it is
	 * a member.
	 * @param c A collection of {@code Integer}-compatible objects.
	 * @return True if the set was changed.
	 */
	@Override
	public boolean retainAll(Collection<?> c) {
		boolean changed=false;
		IntegerSet keep = new IntegerSet(c);
		LinkedList<Integer> list = new LinkedList<Integer>();
		Iterator<Integer> it = iterator();
		Integer i;
		while ( it.hasNext()) {
			if (!keep.contains(i=it.next()))
				list.add(i);
		}
		for (Integer I : list) 
			changed |= remove(I);
		return changed;
	}

	/**
	 * Removes all members from the array, but does NOT reset its boundaries or
	 * ranging mode.
	 */
	@Override
	public void clear() {
		extentSet = new TreeSet<Extent>();
		extentMap = new HashMap<Integer,Extent>();
		autoRange=true;
		minSetValue=UNKNOWN;
		maxSetValue=UNKNOWN;
	}


	/**
	 * Determines if there are no members in this set. Empty
	 * sets have a cardinality (size) of 0.
	 * @return True if there are no members in this set.
	 */
	@Override
	public boolean isEmpty() {
		return size()==0;
	}
	

	/**
	 * Returns the number of members in the set, using a cached value if there is one,
	 * and computing a new value if necessary.
	 * @return The cardinality.
	 */
	@Override
	public int size() {
		if (setCardinality!=null) 
			return setCardinality;
		return computeSetCardinality();
	}
	
	
//======Iterable<Integer> contractual fulfillment
	
	
	/**
	 * Returns an iterator object for traversing the set.  Values
	 * will be returned in ascending order.
	 * @return The iterator.
	 */
	@Override
	public Iterator<Integer> iterator() {
		return new IntegerSetAscendingIterator();
	}	
	
	
//======Additional public methods

	/**
	 * Sets the behavior of auto-ranging for this set.  If false, then
	 * the range is fixed and operations with other sets must be constrained
	 * to other fixed-range sets of the same range.  
	 * 
	 * @param flag True if the set should auto-range.
	 */
	public void autoRanging(boolean flag) {
		autoRange=flag;
	}
	
	/**
	 * Determines if this set is range-compatible with the other set.  Two sets
	 * are range-compatible if and only if they are both auto-ranging, or they 
	 * are both fixed-range and have the same upper and lower bound (same range).
	 * 
	 * @param other The set to be tested for compatibility.
	 * 
	 * @return {@code true} if the other set is range-compatible.
	 */
	public boolean isRangeCompatibleWith(IntegerSet other) {
		return 
			(this.autoRange && other.autoRange) 
			||
			(!this.autoRange && !other.autoRange && this.lowerBound==other.lowerBound && this.upperBound==other.upperBound);
	}
	
	/**
	 * Sets the range for this set.  The current set membership must
	 * fall within the new bounds or an exception will be thrown.
	 * @param lower The new lower bound.
	 * @param upper The new upper bound.
	 */
	public void setRange(int lower, int upper) {
		if (!validBound(lower) || !validBound(upper) || lower>upper)
			throw new RuntimeException("Invalid range");
		if (size()!=0 && ( min()<lower || max()>upper))
			throw new RuntimeException("Incompatible range");
		lowerBound=lower;
		upperBound=upper;
	}
	
	/**
	 * Gets the current upper bound for this set.
	 * @return The lower bound.
	 */
	public Integer getUpperBound() {
		return lowerBound>upperBound ? null :upperBound;
	}

	/**
	 * Gets the current lower bound for this set.
	 * @return The lower bound.
	 */
	public Integer getLowerBound() {
		return lowerBound>upperBound ? null : lowerBound;
	}
	
	/**
	 * The minimum valued member currently in this set,
	 * or null if the set is empty.
	 * 
	 * @return The minimum value or null.
	 */
	public Integer min() {
		//Return cached answer if we have it
		if (minSetValue!=UNKNOWN) 
			return minSetValue;
		if (this.size()==0)
			return null;
		//Iterate in ascending order across the tree
		Iterator<Extent> it = extentSet.iterator();
		Extent e;
		Integer v;
		while (it.hasNext()) {
			e=it.next();
			v = e.min();  //Get the extent's minimum value
			if (v!=null) return minSetValue=v;   //If known, remember and return
		}
		throw new RuntimeException("Programming error - set cardinality is not zero, yet no min was found");
	}
	
	/**
	 * The maximum valued member currently in this set, or
	 * null if the set is empty.
	 * 
	 * @return The maximum value or null.
	 */
	public Integer max() {
		if (maxSetValue!=UNKNOWN) 
			return maxSetValue;
		if (size()==0)
			return null;
		//Iterate in descending order across the tree
		Iterator<Extent> it = extentSet.descendingIterator();
		Extent e;
		Integer v;
		while (it.hasNext()) {
			e = it.next();
			v = e.max();  //Get the extent's maximum value
			if (v!=null) return maxSetValue=v;   //If known, remember and return
		}
		throw new RuntimeException("Programming error - set cardinality is not zero, yet no max was found");
	}
	
	/**
	 * Inserts an array of {@code Integer} values into this set.
	 * 
	 * @param value The array of {@code Integer}s to insert into this set.
	 * 
	 * @return The number of values actually inserted (did not previously exist).
	 */
	public int add(Integer...ints) {
		/*
		 * 1. Is it a value in the universal domain?
		 * 2. If this is a fixed-range set, is the value in bounds?
		 * 3. Do the bounds need to be expanded to accommodate this value?
		 * 4. Get the extent number for this value
		 * 5. Does the extent exist?  If not, add it to the tree set
		 * 6. Insert this value into the extent
		 * 7. If this is really a new member, update cardinality, min, and max
		 */
		
		int returnValue = 0;
		for (int value : ints) {
			size();
			/* 1 */
			if (!validBound(value))
				throw new RuntimeException("Set value not in universal domain");
			
			/* 2 */
			if (!autoRange && (value<lowerBound || value>upperBound))
				throw new RuntimeException("Value out of range");
			
			/* 3 */
			else if (lowerBound>upperBound)
				lowerBound=upperBound=value;
			else if (value<lowerBound) 
				lowerBound=value;
			else if (value>upperBound) 
				upperBound=value;
			
			/* 4 */
			int extentNumber = computeExtent(value);
			
			/* 5 */
			Extent extent = extentMap.get(extentNumber);
			if (extent==null) {
				extent = addExtent(new Extent(extentNumber));
			}
			
			/* 6 */
			if (extent.insert(value)) {
				/* 7 */
				returnValue++;
				setCardinality++;
				if (setCardinality.intValue() == 1) 
					minSetValue=maxSetValue = value;    // If the set was empty, this value is both min and max now
				else {
					//If min or max was known and this value stretches that bound
					if (minSetValue!=UNKNOWN && value<minSetValue) 
						minSetValue=value; 
					if (maxSetValue!=UNKNOWN && value>maxSetValue) 
						maxSetValue=value;
				}
	
			}		
		}
		return returnValue;
	}
	
	/**
	 * Determines the cardinality of the set if every integer in the current
	 * range were a member. A set with no defined range would return 0.
	 * @return The size of the current range.
	 */
	public int rangeSize() {
		return lowerBound>upperBound ? 0 : upperBound-lowerBound+1;
	}
	
	/**
	 * Creates a new set which is the union of this set with
	 * the other set, which is to say, all of the members in
	 * this and the other set.  This set and the other set
	 * remain unchanged.
	 * 
	 * @param other The set to be unioned with this one.
	 * 
	 * @return A new set which is the union of this set and
	 * the other set.
	 */
	public IntegerSet union(IntegerSet other) {
		if (!isRangeCompatibleWith(other))
			throw new RuntimeException("Not range compatible");
		/*
		 * 1. Create a clone of this set
		 * 2. For each extent in the other set
		 * 3.    If the extent exists in this set, 
		 * 4.       Union the two extents
		 * 5.    Else add a clone of the other extent to this set
		 * 6. Invalidate mins and card
		 */
		IntegerSet result = (IntegerSet)clone();
		result.maxSetValue=result.minSetValue=UNKNOWN;
		result.setCardinality=null;
		result.lowerBound=Integer.min(this.lowerBound,other.lowerBound);
		result.upperBound=Integer.max(this.upperBound,other.upperBound);
		//For each extent in the other set
		for (Extent otherExtent : other.extentSet) {
			//If the result (clone of this) already has that extent...
			Extent resultExtent = result.extentMap.get(otherExtent.number);
			if (resultExtent!=null) {
				//Or it's chunks into the result extent's already existant chunks
				for (int x=0; x<CHUNKS_PER_EXTENT; x++) {
					resultExtent.chunks[x] |= otherExtent.chunks[x];
				}
				resultExtent.invalidate();
			} else {
				//We need to add a copy of the other extent to the result's extents
				result.addExtent(result.new Extent(otherExtent,EXTENT_CLONE_FUNCTION.IDENTITY));
			}
		}
		return result;
	}
	
	/**
	 * Creates a new set which is the intersection of this
	 * set with the other set, which is to say, all of the 
	 * members in this set that are also in the other set.
	 * This set and the other set remain unchanged.
	 * 
	 * @param other The set to be intersected with this one.
	 * 
	 * @return A new set which is the intersection of this set
	 * and the other set.
	 */
	public IntegerSet intersect(IntegerSet other) {
		if (!isRangeCompatibleWith(other))
			throw new RuntimeException("Not range compatible");
		/*
		 * 1. Create a new set based on this one
		 * 2. For each extent in this set
		 * 3.    If the extent exists in the other set 
		 * 4.       Create a new extent which is the intersection and add it to the result
		 * 5. Invalidate mins and card
		 */
		IntegerSet result = new IntegerSet(this);
		result.maxSetValue=result.minSetValue=UNKNOWN;
		result.setCardinality=null;
		result.lowerBound=Integer.min(this.lowerBound,other.lowerBound);
		result.upperBound=Integer.max(this.upperBound,other.upperBound);
		for (Extent thisExtent : extentSet) {
			Extent otherExtent = other.extentMap.get(thisExtent.number);
			if (otherExtent!=null) {
				Extent resultExtent = result.new Extent(thisExtent.number);
				for (int x=0; x<CHUNKS_PER_EXTENT; x++)
					resultExtent.chunks[x] = thisExtent.chunks[x] & otherExtent.chunks[x];
				resultExtent.invalidate();
				result.addExtent(resultExtent);
			}
		}
		return result;
	}
	

	
	/**
	 * Creates a new set which is the difference of this set
	 * with the other set, which is to say, all of the members
	 * in this set that are not in the other set.  This set and
	 * the other set remain unchanged.
	 * 
	 * @param other The set whose members are to be subtracted
	 * from the membership of this set to create the new set.
	 * 
	 * @return A new set which is the difference of this set with
	 * the other set.
	 */
	public IntegerSet difference(IntegerSet other) {
		if (!isRangeCompatibleWith(other))
			throw new RuntimeException("Not range compatible");
		/*
		 * 1. Create a new set based on this one
		 * 2. For each extent in this set
		 * 3.   Clone the extent
		 * 4.   If the extent is in the other set
		 * 5.      Difference the extents
		 * 6.   Add the cloned extent to this set
		 * 7. Update boundaries
		 * 8. Invalidate min, max, card
		 */
		IntegerSet result = new IntegerSet(this);
		result.maxSetValue=result.minSetValue=UNKNOWN;
		result.setCardinality=null;
		result.lowerBound=Integer.min(this.lowerBound,other.lowerBound);
		result.upperBound=Integer.max(this.upperBound,other.upperBound);
		for (Extent thisExtent : extentSet) {
			if (paranoid && thisExtent.forcedIsEmpty())
				throw new RuntimeException("PARANOID: encountered empty extent");
			Extent resultExtent = result.new Extent(thisExtent,EXTENT_CLONE_FUNCTION.IDENTITY);
			if (paranoid && resultExtent.forcedIsEmpty())
				throw new RuntimeException("PARANOID: encountered empty extent");
			Extent otherExtent = other.extentMap.get(thisExtent.number);
			if (otherExtent!=null) {
				if (paranoid && otherExtent.forcedIsEmpty())
					throw new RuntimeException("PARANOID: encountered empty extent");
				for (int x=0; x<CHUNKS_PER_EXTENT; x++) {
					resultExtent.chunks[x] &= ~otherExtent.chunks[x];
				}
				resultExtent.invalidate();
			}
			if (resultExtent.extentSize()!=0)
				result.addExtent(resultExtent);
		}
		return result;
	}
	
	/**
	 * Creates a new set with has the same range as this set, but with
	 * a complemented population, that is to say, members in this set are
	 * not in the new set, and members not in this set are in the new set.
	 * This set remains unchanged.
	 * 
	 * @return A new set which is a complement of this one.
	 */
	public IntegerSet complement() {
		IntegerSet result = new IntegerSet(this);
		/* 0. If the range is empty, return the empty set.
		 * 1. BASED ON BOUNDS, determine the first and last logical extents of this set
		 * 2. For each number in the range first..last
		 * 3.    If the extent exists in this set
		 * 4.       Get its complement
		 * 5.       If that is not empty, insert it into the result
		 * 6.    Create an "all 1's" extent for this number and insert it into the set
		 * 7. Invalidate cardinality,min,max
		 */
		if (upperBound<lowerBound)
			return new IntegerSet();
		int firstExtentNumber = computeExtent(lowerBound);
		int lastExtentNumber = computeExtent(upperBound);
		for (int number=firstExtentNumber; number<=lastExtentNumber; number++) {
			Extent extent = extentMap.get(number);
			if (extent!=null) {
				Extent comp = result.new Extent(extent,EXTENT_CLONE_FUNCTION.COMPLEMENT);
				if (comp.extentSize()!=0)
					result.addExtent(comp);
			} else {
				result.addExtent(result.new Extent(number,EXTENT_CLONE_FUNCTION.COMPLEMENT));
			}
		}
		result.minSetValue=maxSetValue=UNKNOWN;
		result.setCardinality=null;
		return result;
	}

	
//------------------------------------------------------------------------------------------	
//-- Private Methods 	
//------------------------------------------------------------------------------------------
	/**
	 * Returns a hexidecimal string representing an int, but treating it as if it were an unsigned integer.
	 * @param i The integer to format.
	 * @return The formatted integer.
	 */
	private String hextostring(int i) {
		char[] s = new char[8];
		for (int x=7; x>=0; x--) {
			byte dig = (byte)(i & 0xF);
			s[x] = (char)(dig>9 ? (dig+'A'-10) : dig+'0');
			i >>>= 4;
		}
		return new String(s);
	}

	private String bintostring(Long l) {
		char[] c = new char[64];
		for (int x=0; x<64; x++) {
			c[x] = l<0 ? '1' : '-';
			l <<= 1;
		}
		return new String(c);
	}
	/**
	 * Inserts the provided extent into the set's data structure.
	 * @param extentNumber
	 * @return
	 */
	private Extent addExtent(Extent extent) {
		extentMap.put(extent.number, extent);
		extentSet.add(extent);			
		return extent;
	}
	
	/**
	 * Removes an extent from the current set if it is a member.
	 * @param extent
	 */
	private void removeExtent(Extent extent) {
		extentMap.remove(extent.number);
		extentSet.remove(extent);
	}

	
	/**
	 * Computes the set cardinality and stores it in setCardinality.
	 * @return The newly computed cardinality.
	 */
	private int computeSetCardinality() {
		int card=0;
		for (Extent e : extentSet)
			card+=e.extentSize();
		return setCardinality=card;
	}

	/**
	 * Given a universal domain value, computes the bit index,
	 * which starts at 0.
	 * @param value
	 * @return
	 */
	private static int computeBit(int value) {
		if (value<0)
			throw new RuntimeException("Set value not in universal domain");
		return value & OFFSET_IN_CHUNK_MASK;
	}
	/**
	 * Given a universal domain value, computes the extent index,
	 * which starts at 0.
	 * @param value
	 * @return
	 */
	private static int computeExtent(int value) {
		if (value<0)
			throw new RuntimeException("Set value not in universal domain");
		return value >>> (CHUNK_INDEX_FIELD_WIDTH+BIT_INDEX_FIELD_WIDTH);
	}
	
	/**
	 * Given a universal domain value, computes the chunk index,
	 * which starts at 0.
	 */
	private static int computeChunk(int value) {
		if (value<0)
			throw new RuntimeException("Set value not in universal domain");
		return (value & OFFSET_IN_EXTENT_MASK) >>> BIT_INDEX_FIELD_WIDTH;
	}
	
	/**
	 * Determines if a value is one of the possible values that can be 
	 * represented by instances of this class.
	 * @param b The value to be tested.
	 * @return True if b is a valid bound.
	 */
	private boolean validBound(int b) {
		return (b>=LOWEST_BOUND && b<=HIGHEST_BOUND);
	}

	private interface pargs {
		public void p(String f, Object...args);
	}
	
	public void dump(PrintStream p) {
	
		pargs P = (s,o) -> { p.println(String.format(s,o)); };
		try {
			P.p("=====CONSTANTS======================================================");
			Class<? extends Object> clazz = this.getClass();
			for (Field f : clazz.getDeclaredFields())
				if ( (f.getModifiers() & (Modifier.FINAL | Modifier.STATIC)) != 0)
					P.p("%30s: %s",f.getName(),
							f.getName().contains("MASK")?("0x"+hextostring((Integer)f.get(this))):f.get(this).toString());
			
			P.p("\n=====VARIABLES======================================================");
			for (Field f : clazz.getDeclaredFields())
				if ( 
					((f.getModifiers() & (Modifier.FINAL | Modifier.STATIC)) == 0)
					&&
					!f.getName().startsWith("extent"))
						P.p("%30s: %s",f.getName(),
							f.getName().contains("MASK")?("0x"+hextostring((Integer)f.get(this))):f.get(this).toString());
			
			P.p("\n=====EXTENTS========================================================");
			P.p("Number of extents in set...: %d", extentSet.size());
			P.p("Number of extents in map...: %d", extentMap.size());
			for (Extent e: extentSet) {
				if (!extentMap.values().contains(e)) P.p("   extent %d in set but not map",e.number);
			}
			for (Extent e: extentMap.values()) {
				if (!extentSet.contains(e)) P.p("   extent %d in map but not in set",e.number);
			}
			int ecount=0;
			int card=0;
			for (Extent e: extentSet) {
				if (extentMap.values().contains(e)) {
					System.out.print(String.format("(%d of %d) ",++ecount,extentSet.size()));
					card=e.dump(card,p);
				}
			}
		} catch (IllegalAccessException e) {
			P.p("Illegal access exception : %s",e.getMessage());
		}

	}
	
//------------------------------------------------------------------------------------------	
//-- Inner class IntegerSetAscendingIterator  	
//------------------------------------------------------------------------------------------
	
	public class IntegerSetAscendingIterator implements Iterator<Integer> {
		
		Iterator<Extent> traverseSetIterator;
		Iterator<Integer> traverseExtentIterator;	
		
		public IntegerSetAscendingIterator() {
			if (size()==0)
				return;
			traverseSetIterator = extentSet.iterator();
			Extent extent = traverseSetIterator.next();
			traverseExtentIterator = extent.iterator();
		}
		
		@Override
		public boolean hasNext() {
			if (traverseExtentIterator.hasNext()) 
				return true;
			return traverseSetIterator.hasNext();
		}
		
		@Override
		public Integer next() {
			if (traverseExtentIterator.hasNext())
				return traverseExtentIterator.next();
			if (traverseSetIterator.hasNext()) {
				traverseExtentIterator = traverseSetIterator.next().iterator();
				return traverseExtentIterator.next();
			}
			throw new NoSuchElementException();
		}
		
		@Override
		public void remove() {
			traverseExtentIterator.remove();
		}
	}
	
	//------------------------------------------------------------------------------------------	
	//-- Inner class ExtentAscendingIterator  	
	//------------------------------------------------------------------------------------------
		
		public class ExtentAscendingIterator implements Iterator<Integer> {
			Extent extent;              // The extent we are traversing
			Integer nextValue;          // The pre-cached value we will vend next()
			Integer returnValue=null;   // The last returned value from next()  
			int chunk;			        // Always the index of the chunk from which nextValue came
			int bit;			        // Always the bit position in that chunk from which nextValue came
			int leftToFind;
			
			
			public ExtentAscendingIterator(Extent extent) {
				this.extent = extent;
				nextValue = extent.min();        //Our first value is the minimum value in the set
				if (paranoid && nextValue==null)			 
					throw new RuntimeException("Empty extent encountered "+extent.forcedIsEmpty());
				bit = computeBit(nextValue);
				chunk = computeChunk(nextValue);
				leftToFind = extent.extentSize();
			}
			
			@Override
			public boolean hasNext() {
				return nextValue!=null;
			}

			@Override
			public Integer next() {
				if (nextValue==null)
					throw new NoSuchElementException();
				returnValue = nextValue;
				nextValue=null;
				long mask = 1L << bit;
				if (leftToFind>0)
					do {
						// Advance bit
						bit++;
						// If wrapped...
						if (bit>=BITS_PER_CHUNK) {
						//  Advanced to next chunk
							chunk++;
						// 	If wrapped, break (end of iteration)
							if (chunk==CHUNKS_PER_EXTENT)
								break;
						//  If this chunk is zero, continue
							if (extent.chunks[chunk]==0)
								continue;
						//  Reset bit and mask to traverse this non-empty new chunk
							bit=0;
							mask=1L;
						// Else, advance the mask
						} else
							mask<<=1;
						// If bit is set in chunk
						if ((extent.chunks[chunk]&mask) !=0) {
						//     Set nextValue
							nextValue=extent.firstvalue + chunk*BITS_PER_CHUNK + bit;
							leftToFind--;
						//     Break
							break;
						}
					} while (true);
				return returnValue;
			}
		}
//------------------------------------------------------------------------------------------	
//-- Inner class Extent 	
//------------------------------------------------------------------------------------------
	
	/**
	 * Implements the largest unit of allocation for the IntegerSet parent class.
	 */
	private class Extent implements Comparable<Extent>, Iterable<Integer>{
		//This class does not check parameters coming in, since it is private and
		//called only from the parent class, which is checking all parameters.
		
		/** The extent number, starting with 0. */
		public int number;
		
		/** The first bit in the extent represents this value. */
		public int firstvalue;
		
		/** The last bit in the extent represents this value. */
		public int lastvalue;
		
		/** This is the collection of chunks that actually are the bit flags for the member values. */
		public long[] chunks;
		
		/** The minimum and maximum valued members in the set, if known. */
		private Integer minExtentValue = null;  //The minimum value stored in this extent or unknown
		private Integer maxExtentValue = null;  //The maximum value stored in this extent or unknown
		
		/** The number of member values in the set - the number of bits that are turned "on" */
		private Integer extentCardinality = 0;   //The number of values stored in this extent if it is known
		
		
		/**
		 * Creates a new extent with the specified index and initialization value.
		 *  @param number The extent index number.
		 *  @param function If IDENTITY, then the extent is initialized to empty.  If COMPLEMENT, then the
		 *  extent is initialized to full (all members marked as "in").
		 */
		public Extent(int number, EXTENT_CLONE_FUNCTION function) {
			super();
			this.number=number;
			chunks = new long[CHUNKS_PER_EXTENT];   //We take the default of all zeros, since this is initially an empty chunk
			firstvalue = number*VALUES_PER_EXTENT;          //We cache this value so we don't have to keep recomputing it (invariant for this extent number)
			lastvalue = (number+1)*VALUES_PER_EXTENT-1;
			if (function==EXTENT_CLONE_FUNCTION.COMPLEMENT) {
				applyComplementation();
			} else {
				invalidate();
			}
		}

		/**
		 * Creates a new extent with the specified index and initialized to empty.
		 * @param number The extent index number.
		 */

		public Extent(int number) {
			this(number,EXTENT_CLONE_FUNCTION.IDENTITY);
		}

		/**
		 * Creates a new extent that is a clone of another extent, except possibly in its affinity for an outer class
		 * instance, which will be the set instance that calls this constructor.
		 * @param other The other extent from which to clone this one.
		 * @param complement If true, the cloned extent will be the complement of the original.
		 */
		public Extent(Extent other, EXTENT_CLONE_FUNCTION function) {
			this(other.number);
			for (int x=0; x<CHUNKS_PER_EXTENT;x++)
				chunks[x]=other.chunks[x];
			if (function==EXTENT_CLONE_FUNCTION.COMPLEMENT)
				applyComplementation();
			else {
				minExtentValue=other.minExtentValue;
				maxExtentValue=other.maxExtentValue;
				extentCardinality = other.extentCardinality;
			}
		}

		
		/**
		 * Returns the minimum value in this extent, or null if it is empty.
		 * @return
		 */
		public Integer min() {
			/* We cache the min and max values.  If they are not known
			 * we perform a search of the chunks array.
			 */
			if (minExtentValue!=null) 
				return minExtentValue;
			for (int x=0; x<CHUNKS_PER_EXTENT; x++)   	//For the min, search from the front to the back
				if (chunks[x]!=0) {  					  //Quick test to see if there are any values in this chunk
					int b=0; 								//Test each bit position from LSB to MSB
					for (long mask=1; mask!=0; mask<<=1,b++) { 		   
						if ( (chunks[x] & mask) !=0 )
							return minExtentValue = firstvalue + x*VALUES_PER_CHUNK + b;  //Return value and remember it
					}
				}
			maxExtentValue=minExtentValue=null;  //The extent is empty, so there can be no min or max
			return null;
		}

		/**
		 * Returns the minimum value in this extent, or null if it is empty
		 * @return
		 */
		public Integer max() {
			/* We cache the min and max values.  If they are not known
			 * we perform a search of the chunks array.
			 */
			if (maxExtentValue!=null) 
				return maxExtentValue;
			for (int x=CHUNKS_PER_EXTENT-1; x>=0; x--) 				//For the max, search from back to front
				if (chunks[x]!=0) { 					  			  //Quick test to see if there are any values in this chunk
					int b=VALUES_PER_CHUNK-1; 								//Test each bit position from MSB to LSB
					for (long mask=Long.MIN_VALUE; mask!=0; mask>>>=1, b--) { 	   
						if ( (chunks[x] & mask) !=0 )
							return maxExtentValue=firstvalue + x*VALUES_PER_CHUNK + b; //Return value and remember it
					}
				}
			maxExtentValue=minExtentValue=null;   //The extent is empty, so there can be no min or max
			return null;
		}

		
		/**
		 * Insert a value into the extent.  More precisely, set the bit in the extent that represents this value 
		 * to {@code 1}.
		 * @param value  The integer value to mark as being included. This is the value as drawn from the universal domain.
		 *    This routine does not check to see if {@code value} actually belongs in this extent, as it only looks at the
		 *    lower order bits for chunk and bit indices.
		 * @return True if the value was not previously a member.
		 */
		public boolean insert(int value) {
			//We assume the value goes in this chunk - the parent must guarantee this
			int chunkidx = computeChunk(value);  // What chunk in the chunks array?
			long mask = 1L << computeBit(value);  // Create a mask with a 1 where the value's flag goes
			if ((chunks[chunkidx] & mask)!=0) 
				return false; //If anding this with the chunk is non-zero, the value is already "in"
			extentSize();  //Insure cardinality is up to date - doing it here could avoid an iteration over the chunks
			chunks[chunkidx] |= mask;  // Or this value's flag into the chunk
			extentCardinality++;  // The number of values in the extent is definitely incremented
			if (extentCardinality.intValue()==1) 
				minExtentValue=maxExtentValue = value;    // If the extent was empty, this value is both min and max now
			else {
				//If min or max was unknown and this value stretches that bound
				if (minExtentValue!=null && value<minExtentValue) 
					minExtentValue=value; 
				if (maxExtentValue!=null && value>maxExtentValue) 
					maxExtentValue=value;
			}
			return true;
		}

		/**
		 * Removes a value from the extent.  More precisely, set the bit in the extent that represents this value 
		 * to {@code 0}.
		 * @param value  The integer value to mark as being excluded. This is the value as drawn from the universal domain.
		 *    This routine does not check to see if {@code value} actually belongs in this extent, as it only looks at the
		 *    lower order bits for chunk and bit indices.
		 * @return True if the value was previously a member.
		 */		
		public boolean remove(int value) {
			//We assume the value goes in this chunk - the parent must guarantee this
			int chunkidx = computeChunk(value);  // What chunk in the chunks array?
			long mask = 1L << computeBit(value); // Create a mask with a 1 where the value's flag goes
			if ((chunks[chunkidx] & mask)==0) 
				return false;  //Return false if already not in the set
			extentSize();   //Ensure cardinality is up to date. Doing it here could avoid iterating over the chunks later
			chunks[chunkidx] &= ~mask;  //Remove flag from chunk by anding with the complement of the mas
			extentCardinality--;       //Definitely losing a value
			if (extentCardinality.intValue()==0) {  // If no more values, min and max go to UNKNOWN
				minExtentValue=maxExtentValue=null;
			} else {
				//Otherwise, if we just removed the min or max, that value is now considered UNKNOWN
				if (minExtentValue!=null && minExtentValue.intValue()==value) minExtentValue=null;
				if (maxExtentValue!=null && maxExtentValue.intValue()==value) maxExtentValue=null;
			}
			return true;
		}
		
		public boolean isSet(int value) {
			//We assume the value goes in this chunk - the parent must guarantee this
			int chunkidx = computeChunk(value);  // What chunk in the chunks array?
			long mask = 1L << computeBit(value);  // Create a mask with a 1 where the value's flag goes
			return (chunks[chunkidx] & mask)!=0;
		}
		
		@Override
		public boolean equals(Object obj) {
			return number==((Extent)obj).number;
		}
		
		public int compareTo(Extent b) {
			return number-b.number;
		}
		
		public Iterator<Integer> iterator() {
			return new ExtentAscendingIterator(this);
		}
		
		public int extentSize() {
			if (extentCardinality!=null)
				return extentCardinality;
			int card = 0;
			for (int n=0; n<CHUNKS_PER_EXTENT; n++)
				card += Long.bitCount(chunks[n]);
			return extentCardinality = card;
		}
		
		
	
		/**
		 * Creates a complemented clone of this extent, automatically using the parent set's
		 * lowerBound and upperBound values to determine left and right boundaries.
		 * @return
		 */
		private void applyComplementation() {
			int leftChunk=0;
			int lowBit=0;
			int rightChunk=CHUNKS_PER_EXTENT-1;
			int highBit=BITS_PER_CHUNK-1;
			//Remember, lowerBound<=upperBound is true and is invariant whenever this routine is called.
			//Otherwise, the set is not just empty, but it has an empty range as well, and no extents would be possible.
			//This means there MUST be at LEAST one bit in the range.
			
			//If the lowerBound is higher than the first value in this extent, we need to
			//recompute low*
			if (firstvalue < lowerBound) {
				leftChunk=computeChunk(lowerBound);
				lowBit=computeBit(lowerBound);
			}
			//Similary for the upperBound
			if (lastvalue > upperBound) {
				rightChunk=computeChunk(upperBound);
				highBit=computeBit(upperBound);
			}
			//Special boundary case is when leftChunk and rightChunk are the same
			if (leftChunk==rightChunk) {
				//We need to create a mask that includes highBit to lowBit
				//                                      7 6 5 4 3 2 1 0
				// Using an N=8-bit chunk for example:  - - - - - - - -
				// lowBit=3 highBit=5                       H   L
				// the mask would be                    0 0 1 1 1 0 0 0
				// the width W of the mask is H-L+1 (3)     1 2 3
				//
				// Starting with all 1's                1 1 1 1 1 1 1 1
				// >>lowBit N-W  (5)                    0 0 0 0 0 1 1 1
				// <<lowBit                             0 0 1 1 1 0 0 0
				int W = highBit-lowBit+1;
				long mask = (-1L >>> (VALUES_PER_CHUNK-W))<<lowBit;
				chunks[leftChunk] = (~chunks[leftChunk])&mask;
			} else {
				for (int chunk=leftChunk; chunk<=rightChunk; chunk++) {
					if (chunk==leftChunk && lowBit!=0) {
						//We need to mask (force to 0) anything BELOW the lowBit
						//Using an N=8-bit chunk for example
						//If lowBit=3, then the mask should look like  11111000
						//                         Position of the low bit ^
 						//This would be all 1's << lowBit
						chunks[chunk] = (~chunks[chunk]) & (-1L<<lowBit);
						
					} else if (chunk==rightChunk && highBit!=VALUES_PER_CHUNK-1) {
						//We need to mask (force to 0) everything ABOVE the highBit
						//Using an N=8-bit chunk for example
						//If highBit=6, then the mask should look like 01111111    8-6=2  -1 =1
						//                    Position of the high bit  ^      
						//If highBit=3, then the mask should look like 00001111    8-3=5  -1 =4
						//                    Position of the high bit     ^      
						//If highBit=2, then the mask should look like 00000111    8-2=6  -1 =5
						//                    Position of the high bit      ^      
						//That means shift all 1's left N-highBit-1
						chunks[chunk] = (~chunks[chunk]) & (-1L>>>(VALUES_PER_CHUNK-highBit-1));
					} else {
						chunks[chunk] = ~chunks[chunk];
					}
				}
			}
			invalidate();
		}
		
		public int dump(int card, PrintStream p) {
			
			pargs P = (s,o) -> { p.println(String.format(s,o));};
			
			P.p("\n--- extent #%d---", number);
			P.p("firstvalue.............: %d", firstvalue);
			P.p("lastvalue..............: %d", lastvalue);
			P.p("minExtentValue.........: %d", minExtentValue);
			P.p("maxExtentValue.........: %d", maxExtentValue);
			P.p("extentCardinality......: %s", extentCardinality);
			
			boolean trip=false;
			for (int chunk=0; chunk<CHUNKS_PER_EXTENT; chunk++)
				if (chunks[chunk]!=0) {
					int linecard=Long.bitCount(chunks[chunk]);
					card+=linecard;
					P.p("   chunk %3d: %2d %10d  %s  %s",linecard, card, chunk, bintostring(chunks[chunk]), chunkValues(chunks[chunk],firstvalue+chunk*BITS_PER_CHUNK));
					trip=true;
				}
			if (!trip)
				P.p("The extent is empty - none of the chunks are non-zero");
			return card;
		}

		private String chunkValues(long chunk, int base) {
			if (chunk==0) return "";
			StringBuilder sb = new StringBuilder();
			int b=0;
			for (long mask=1; mask!=0; mask<<=1,b++) {
				if ((chunk & mask) !=0) {
					if (sb.length()!=0)
						sb.append(" ");
					sb.append(String.valueOf(base+b));
				}
			}
			return sb.toString();
		}
		
		private void invalidate() {
			extentCardinality=null;
			minExtentValue=maxExtentValue=null;
		}

		private boolean forcedIsEmpty() {
			invalidate();
			return extentSize()==0;
		}
		
	}


	

	
	
}
