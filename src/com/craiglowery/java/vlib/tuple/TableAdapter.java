package com.craiglowery.java.vlib.tuple;

import java.util.LinkedList;
import java.util.List;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.tuple.filterexp.TupleExpressionFactory;
import com.craiglowery.java.vlib.tuple.filterexp.FilterExpressionException;

/**
 * An abstract class from which Tuple-specific table adapters are derived.
 * 
 *
 */
public abstract class TableAdapter<T extends Tuple> {

	PersistenceConnection store = null;
	
	/** A reference to the serviced class's type */			 public Class<? extends Tuple> servicedClass;
	/** A reference to the name of the serviced class */	 public String servicedClassName;
	/** An expression factory for use with this type */		 public TupleExpressionFactory EF;

	protected TupleSubClassReflectedData RD = null;
	
	public TableAdapter(Class<? extends Tuple> tupleClassToService, PersistenceConnection store) throws U_Exception {
		servicedClass=tupleClassToService;
		servicedClassName=servicedClass.getName();
		Tuple.registerSubclass(tupleClassToService);
		RD = Tuple.getReflectedData(servicedClass);
		try {
			EF = new TupleExpressionFactory(servicedClass);
		} catch (
				FilterExpressionException | 
				SecurityException | 
				IllegalArgumentException e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
		this.store = store;
	}
	
	

	public boolean isInPrimaryKey(String name) {
		return RD.primaryKeysIndex.containsKey(name);
	}
	
	public boolean isAttribute(String name) {
		return RD.attributesIndex.containsKey(name);
	}

	public int attributeOffset(String name) {
		return RD.attributesIndex.get(name);
	}
	
	public boolean isDefaultOnInsert(String name) {
		return RD.defaultOnInsertIndex.containsKey(name);
	}
	
	public int numberOfPrimaryKeys() {
		return RD.primaryKeysIndex.size();
	}
	
	public int numberOfAttributes() {
		return RD.attributeNames.size();
	}
	
	public Tuple.Type tupleTypeOf(int attributeOffset) {
		return RD.tupleTypes.get(attributeOffset);
	}
	
	public Tuple.Type tupleTypeOf(String attributeName) {
		Integer attributeIndex =RD.attributesIndex.get(attributeName);
		if (attributeIndex==null)
			return null;
		return RD.tupleTypes.get(attributeIndex);
	}
	
	public Iterable<String> attributeNamesIterable() {
		return RD.attributeNames;
	}

	public String[] attributesArray() {
		return RD.attributeNames.toArray(new String[RD.attributeNames.size()]);
	}
	
	public Iterable<String> primaryKeyNamesIterable() {
		return RD.primaryKeysIndex.keySet();
	}
	
	public String[] primaryKeysArray() {
		return RD.primaryKeysIndex.keySet().toArray(new String[RD.primaryKeysIndex.size()]);
	}
	
	public Class<?> javaTypeOf(int attributeOffset) {
		return RD.javaTypes.get(attributeOffset);
	}
	
	public Class<?> javaTypeOf(String attributeName) {
		return RD.javaTypes.get(RD.attributesIndex.get(attributeName));
	}
		
	public String getClassName() {
		return servicedClassName;
	}
	
	/**
	 * Generate a tuple filter expression that can be used to identify a tuple by primary key.
	 * @param t
	 * @return
	 * @throws U_Exception
	 */
	public TupleExpressionFactory.Node getPrimaryKeyExpression(T t) throws U_Exception {
		try {
			String keys[] = primaryKeysArray();
			// If there is no primary key defined, then no tuple can be matched
			if (keys.length==0) 
				return EF.FALSE;
			// If there is one attribute in the key set...
			if (keys.length==1) 
				return EF.comp(EF.attribute(keys[0]), "=", t.getAttributeValue(keys[0]));
			// If there are more than one, we need to and them together
			// First, make a list of the nodes to be anded
			List<TupleExpressionFactory.Node> andedNodes = new LinkedList<TupleExpressionFactory.Node>();
			// Add each node to the list
			for (int x=0; x<keys.length; x++) {
				andedNodes.add(EF.comp(EF.attribute(keys[x]),"=",t.getAttributeValue(keys[x])));
			}
			// Convert the list to an array of nodes, and use that to construct the And node
			TupleExpressionFactory.Node[] narr = new TupleExpressionFactory.Node[0];
			narr=andedNodes.toArray(narr);
			return EF.and((Object[])narr);
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,"Could not generate primary key expression for "+servicedClassName);
		}
		
	}
	
	

	/**
	 * Returns a list of tuples from the associated backing table using a filtering expression.
	 * 
	 * @param filter  An expression tree built by an ExpressionFactory<T> which will be evaluated for
	 * each row of the table. Only rows that evaluate to {@code true} are included in the result.
	 * A value of {@code null} means return every row.
	 * 
	 * @param sort A {@code SortDirective} array that describes how rows should be ordered. If null,
	 * then the primary keys are used in ascending order, in the order in which they are declared
	 * in class {@code <T>}.  If {@code null}, then the table is sorted by primary keys ascending.
	 * 
	 * @param limit The maximum number of rows to return. If {@code 0} then the maximum number of
	 * rows allowed by the implementation of the derived class is the limit.
	 * 
	 * @param xform An object that implements the method {@code action(T tuple)}. If non-null,
	 * then this object's method will be called for each tuple retrieved.
	 * 
	 * @return If {@code xform} is null, then a list of tuples retrieved is returned.
	 * if {@code xform} is not-null, then it is assumed the SelectionTransformer object has
	 * collected the results appropriately, and null is returned by this method.
	 */
	public abstract List<T> select(
			TupleExpressionFactory.Node filter, 
			SortDirective[] sort, 
			int limit,
			SelectionTransformer<T> xform) throws U_Exception; 
	
	/**
	 * Returns a list of all tuples from the backing table in default sort order.
	 * @return The list of tuples.
	 * @throws U_Exception
	 */
	public List<T> select() throws U_Exception  {
		return select(EF.TRUE,null,0,null);
	}
	
	/**
	 * Returns a list of tuples from the backing table in default sort that
	 * match the expression respresented by the string.
	 * @param filterExpression  A textual representation of the filter expression.
	 * @return A list of Tuples.
	 * @throws U_Exception
	 */
	public List<T> select(String filterExpression) throws U_Exception {
		try {
			return select(EF.parse(filterExpression));
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.ExpressionError,e);
		}
	}

	/**
	 * Returns a list of tuples from the backing table that match the filter.
	 * Rows are returned in default sort order and no limit is applied.
	 * @param filter The filter node which roots an expression tree to evaluate.
	 * @return The list of tuples.
	 * @throws U_Exception
	 */
	public List<T> select(TupleExpressionFactory.Node filter) throws U_Exception {
		return select(filter,null,0,null);
	}
	
	/**
	 * Returns a list of all tuples from the backing table in the sort order
	 * specified.
	 * @param sort The sort directive. If null, a default sort order is used.
	 * @return The list of tuples.
	 * @throws U_Exception
	 */
	public List<T> select(SortDirective[] sort) throws U_Exception {
		return select(EF.TRUE,sort,0,null);
	}

	/**
	 * Returns the first {@code limit} number of tuples from the backing table
	 * in the default sort order.
	 * @param limit The number of rows to return, or 0 for all rows.
	 * @return The list of tuples.
	 * @throws U_Exception
	 */
	public List<T> select(int limit) throws U_Exception {
		return select(EF.TRUE,SortDirective.NONE,limit,null);
	}
	
	/**
	 * Return a list of tuples that match the provided filter, sorted as specified.
	 * There is no limit on the size of the result set other than that imposed by
	 * the implementation.
	 * @param filter The root node of the expression tree to be used as a filter,
	 * or null for all rows.
	 * @param sort A sort directive array, or null for the default order.
	 * @return The list up tuples.
	 * @throws U_Exception
	 */
	public List<T> select(TupleExpressionFactory.Node filter, SortDirective[] sort) throws U_Exception {
		return select(filter,sort,0,null);
	}

	/**
	 * Return a size-limited list of tuples that match the provided filter, sorted by the default rule.
	 * @param filter The root node of the expression tree to be used as a filter,
	 * or null for all rows.
	 * @param limit The number of rows to return, or 0 for all rows.
	 * @return The list up tuples.
	 * @throws U_Exception
	 */
	public List<T> select(TupleExpressionFactory.Node filter, Integer limit) throws U_Exception {
		return select(filter,null,limit,null);
 	}
	
	/**
	 * This method pulls a filtered selection from the table and calls a transformation
	 * function on each tuple.
	 * @param tupleFilter The filter governing the selection, or null for all tuples in the table.
	 * @param xform An object that implements the {@code SelectionTransformationFunction<T>} interface,
	 *   which must include an action method that accepts a tuple of type T.  If the action method
	 *   returns true, then this method will continue to the next tuple in the source if there
	 *   is one.  Returning false causes traversal of the table to cease and is a way that 
	 *   the action method can cancel traversal in the middle of it necessary.
	 * @throws U_Exception
	 */
	public void applySelection(TupleExpressionFactory.Node tupleFilter,
			SelectionTransformer<T> xform) throws U_Exception {
		select(tupleFilter,null,0,xform);
	}
	
	/**
	 * This method pulls a filtered selection from the table and calls a transformation
	 * function on each tuple.
	 * @param tupleFilter The filter governing the selection, or null for all tuples in the table.
	 * @param sort A sort directive.
	 * @param limit The maximum number of tuples to be returned.
	 * @param xform An object that implements the {@code SelectionTransformationFunction<T>} interface,
	 *   which must include an action method that accepts a tuple of type T.  If the action method
	 *   returns true, then this method will continue to the next tuple in the source if there
	 *   is one.  Returning false causes traversal of the table to cease and is a way that 
	 *   the action method can cancel traversal in the middle of it necessary.
	 * @throws U_Exception
	 */	public void applySelection(
			TupleExpressionFactory.Node tupleFilter,
			SortDirective[] sort,
			int limit,
			SelectionTransformer<T> xform)  
		throws U_Exception 
	{
		select(tupleFilter,sort,limit,xform);
	}
	

	public abstract int delete(TupleExpressionFactory.Node filter) throws U_Exception;
	
	public abstract boolean delete(T t) throws U_Exception;
	
	public abstract boolean update(T t) throws U_Exception;
	
	public abstract void insert(T t) throws U_Exception;
	
	public abstract void insertIfNew(T t) throws U_Exception;

		

	
}
