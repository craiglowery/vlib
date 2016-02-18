
package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.craiglowery.java.vlib.common.Config;
import com.craiglowery.java.vlib.common.ConfigurationKey;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.common.Util;
import com.craiglowery.java.vlib.tuple.SortDirective.Order;
import com.craiglowery.java.vlib.tuple.filterexp.TupleExpressionFactory;
import com.craiglowery.java.vlib.tuple.filterexp.FilterExpressionException;
import com.craiglowery.java.vlib.tuple.filterexp.PostgresqlExpressionGenerator;
import com.craiglowery.java.vlib.tuple.Tuple.Type;

/**
 * This is a derived class the implements TableAdapter for Postgresql
 * databases.
 * @param <T> The type of tuple that populates the table to be backed.
 */
public class PostgresqlTableAdapter<T extends Tuple> extends TableAdapter<T> {

	String tablename=null;
	Connection db = null;
	int maxTableResultSize = 1024*10;  //This can be overridden in the configuration file
	
	/** Comma separated list of all SQL attribute names for this tuple **/
	private String selectAttributes=null;
	
	/** Array of SQL attribute names for this tuple, in the order tracked by the reflection object **/
	private String[] selectAttributesArray=null;
	
	/** Comma separated list of all SQL attribute names that should be provided values upon INSERT **/
	private String insertAttributes=null;
	
	/** Array of SQL attribute names that should be provided values upon INSERT **/
	private String[] insertAttributesArray=null;
	
	/** Comma separated list of ? placeholders used in inserts. Mirrors/matches {@code insertAttributes}. **/
	private String insertValues=null;
	
	/** An array of field offsets for insert values. {@code insertValueOffset[x]} returns the field offset
	 * for the {@code x}th insert field, where x=0 is the first field in the {@code insertAttributes}
	 * list, and {@code insertAttributesArray[x]} is that attribute's name.
	 */
	private int[]  insertValueOffset=null;
	
	/** SQL boolean expression that selects a tuple based on its primary key values.  If there are n
	 * primary keys, then the expression would be "keyname0=? AND keyname1=? AND ... keyname(n-1)=?".
	 */
	private String selectWhere=null;
	
	/** An array of field offsets for the selectWhere clause. {@code insertValueOffset[x]} returns the 
	 * field offset for the {@code x}th insert field, where x=0 is the first field in the 
	 * {@code insertAttributes} list, and {@code insertAttributesArray[x]} is that attribute's name.
	 */
	private int[]  whereValueOffset=null;
	
	/** Comma separated list of primary key attribute names. Used in the ON CONFLICT clause of INSERT. **/
	private String insertKeys=null;
	
	/** Array of SQL attribute names comprising the table's unique key index. **/
	private String[] insertKeysArray=null;
	
	/** A prepared PostgresSQL statement for performing INSERT operations that will fail on duplicate. **/
	private String INSERT_COMMAND=null;
	
	/** A prepared PostgresSQL statement for performing INSERT operations that do not fail on duplicate. **/
	private String INSERT_COMMAND_ON_CONFLICT=null;
	
	/** A prepared PostgresSQL statement for performing SELECT using a primary key to identify a single tuple. **/
	private String SELECT_BY_KEY=null;
	
	public PostgresqlTableAdapter(Class<? extends Tuple> tupleSubClass, PersistenceConnection store,  String tablename, boolean strict) throws U_Exception {
		super(tupleSubClass, store);
		db = ((PostgresqlConnection)store).db;
		this.tablename = tablename;
		Integer mtrs = Config.getInt(ConfigurationKey.MAX_TABLE_RESULT_SIZE);
		if (mtrs!=null)
			maxTableResultSize = mtrs;
		
		//For later performant lookup of field offsets, we will create fixed-length arrays of
		//primitive ints and Strings.  However, we don't know the lengths of these arrays at this
		//point, and will learn during the traversal of reflected data about the tuple.  We will
		//collect relevant information in linked lists and convert them after the loop to
		//arrays that can be referenced in other methods.
		List<String> selectAttributesL = new LinkedList<String>();
		List<String> insertAttributesL = new LinkedList<String>();
		List<String> insertKeysL = new LinkedList<String>();
		List<Integer> whereValueOffsetL = new LinkedList<Integer>();
		List<Integer> insertValueOffsetL = new LinkedList<Integer>();
		
		for (int a = 0; a<RD.attributeNames.size(); a++) {
			String attributeName = RD.attributeNames.get(a);
			//We are stepping through the fields in the tuple in the order the reflected data in RD
			//keeps them.  We need to create statement fragments for SQL queries as follows:
			//
			//  selectAttributes: The list of all attributes names for SELECT:  ATTR, ATTR, ...
			selectAttributes = Util.commaAppend(selectAttributes, attributeName);
			selectAttributesL.add(attributeName);
			//  insertAttributes: The list of attribute names that get values on INSERT: ATTR1, ATTR2, ...
			//  insertValues:     The list of values for the INSERT commands:  ?a1, ?a2, ... 
			if (!isDefaultOnInsert(attributeName)) {
				insertAttributesL.add(attributeName);
				insertAttributes=Util.commaAppend(insertAttributes, attributeName);
				insertValues=Util.commaAppend(insertValues, "?");
				insertValueOffsetL.add(a);
			}
			//  selectWhere:      The boolean expression for WHERE based on primary keys  KEY1=k1? AND KEY2=k2?
			//  insertKeys:       The list of key names for use in ON CONFLICT clause
			if (isInPrimaryKey(attributeName)) {
				selectWhere=Util.spacerAppend(selectWhere, String.format("%s=?", attributeName), " AND ");
				insertKeys=Util.commaAppend(insertKeys, attributeName);
				insertKeysL.add(attributeName);
				whereValueOffsetL.add(a);
			}
		}
		selectAttributesArray=selectAttributesL.toArray(new String[0]);
		insertAttributesArray=insertAttributesL.toArray(new String[0]);
		insertKeysArray=insertKeysL.toArray(new String[0]);
		insertValueOffset = new int[insertValueOffsetL.size()];
		int x=0;
		for (Integer I : insertValueOffsetL) insertValueOffset[x++]=I;
		whereValueOffset = new int[whereValueOffsetL.size()];
		x=0;
		for (Integer I : whereValueOffsetL) whereValueOffset[x++]=I;
		
		INSERT_COMMAND = new StringBuilder()
				.append("INSERT INTO ")
				.append(tablename)
				  .append(" (")
				  .append(insertAttributes).
				  append(") ")
				.append(" VALUES (")
				  .append(insertValues)
				  .append(")")
				.append(";")
				.toString();

		INSERT_COMMAND_ON_CONFLICT = 
				insertKeys==null? INSERT_COMMAND :    //There are no keys, so no ON CONFLICT needed
				new StringBuilder()
				.append("INSERT INTO ")
				.append(tablename)
				  .append(" (")
				  .append(insertAttributes).
				  append(") ")
				.append(" VALUES (")
				  .append(insertValues)
				  .append(")")
				.append(" ON CONFLICT (")
				  .append(insertKeys)
				  .append(") DO NOTHING")
				.append(";")
				.toString();
		
		SELECT_BY_KEY =
				new StringBuilder()
				.append("SELECT ")
				.append(insertAttributes)
				.append(" FROM ")
				.append(tablename)
				.append(" WHERE ")
				.append(selectWhere==null?"TRUE":selectWhere)
				.append(";")
				.toString();
		
		vetTable(strict);
	}
	
	/**
	 * Determines if the Tuple type matches the named table as follows:<p>
	 * 
	 * <ul>
	 *   <li> Do the primary keys match?
	 *   <li> Is every attribute present?
	 *   <li> If strict is {@code true}, is every attribute in the table represented?
	 * </ul>
	 * 
	 * @param strict if {@code true}, then insists that every attribute in the table be mapped into an
	 * attribute in the tuple.
	 * @throws U_Exception
	 */
	private void vetTable(boolean strict) throws U_Exception {
		
		
		//Let's make a clone of the attribute list.  We'll remove names from it as we
		//encounter them
		
		Set<String> copyAttributes = new HashSet<String>();
		for (String name : attributeNamesIterable())
			copyAttributes.add(name);
		
		//We'll use this list to record attributes in the result set that have no match in
		//the tuple.
		List<String> orphans = new LinkedList<String>();
		
		try ( Statement st = ((PostgresqlConnection)store).db.createStatement()) {
			ResultSet rs = st.executeQuery("SELECT * FROM "+tablename+" LIMIT 1;");
			ResultSetMetaData rsmd = rs.getMetaData();
			int columns = rsmd.getColumnCount();
			for (int column=1; column<=columns; column++) {
				//Is this column one of the keys or attributes?  Or is it an orphan?
				String name = rsmd.getColumnName(column);
				if (copyAttributes.contains(name)) {
					//Check the types - they must be compatible in the backing store and the tuple
					Type tupleSays = tupleTypeOf(name);
					if (tupleSays==null)
						throw new U_Exception(U_Exception.ERROR.Unexpected,
							String.format("Field '%s' in '%s' has an unmapped type", name,servicedClassName));
					Type postgresSays = classifySqlTypeClass(rsmd.getColumnType(column));
					if (postgresSays==null)
						throw new U_Exception(U_Exception.ERROR.Unexpected,
							String.format("Column '%s' in table '%s' has an unmapped type", name,tablename));
					if (tupleSays!=postgresSays)
						throw new U_Exception(U_Exception.ERROR.Unexpected,
								String.format("Column/attribute '%s' type does not agree between '%s'(%s) and table '%s'(%s)", 
										name,servicedClassName,tupleSays.name(),tablename,postgresSays.name()));
					copyAttributes.remove(name);
				} else
					orphans.add(name);
			}
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"Could not vet table "+tablename,e);
		}
		
		//At this point, we have reviewed all the columns the database knows about. If there are any
		//names left in the attributes set, then there is a problem
		
		if (copyAttributes.size()>0) {
			String[] s = new String[0];
			s = copyAttributes.toArray(s);
			StringBuilder sb = new StringBuilder(s[0]);
			for (int x=1; x<s.length; x++)
				sb.append(" ").append(s[x]);
			throw new U_Exception(U_Exception.ERROR.Unexpected,
					String.format("Attributes in '%s' not found in table '%s': %s",servicedClassName,tablename, sb.toString()));
		}
		
		//If we've been asked to be strict and there are any orphans, we must fail
		if (strict && orphans.size()>0) {
			String[] s = new String[0];
			s = orphans.toArray(s);
			StringBuilder sb = new StringBuilder(s[0]);
			for (int x=1; x<s.length; x++)
				sb.append(" ").append(s[x]);
			throw new U_Exception(U_Exception.ERROR.Unexpected,
					String.format("Attributes in table '%s' not found in '%s': %s",tablename,servicedClassName, sb.toString()));
		}
		
	}
	
	private static Type classifySqlTypeClass(int t) {
		switch (t) {
		case java.sql.Types.BIGINT: return Type.Long;
		case java.sql.Types.BIT:
		case java.sql.Types.BOOLEAN: return Type.Boolean;
		case java.sql.Types.DOUBLE: 
		case java.sql.Types.REAL:
		case java.sql.Types.FLOAT:  return Type.Double;
		case java.sql.Types.INTEGER: return Type.Integer;
		case java.sql.Types.CHAR:
		case java.sql.Types.LONGNVARCHAR:
		case java.sql.Types.LONGVARCHAR:
		case java.sql.Types.NCHAR:
		case java.sql.Types.VARCHAR:
		case java.sql.Types.NVARCHAR: return Type.String;
		case java.sql.Types.TIMESTAMP_WITH_TIMEZONE: 
		case java.sql.Types.TIMESTAMP: return Type.Instant;
		default: return null;
		
		}
	}
	

	
	public List<T> select(
			TupleExpressionFactory.Node filter, 
			SortDirective[] sort, 
			int limit,
			SelectionTransformer<T> xform) throws U_Exception {
		try {
			String sfilter = filter==null ? "true" : PostgresqlExpressionGenerator.expand(filter);
			String ssort = "";
			if (sort==null) {
				sort = new SortDirective[RD.primaryKeysIndex.size()];
				int x=0;
				for (String key : primaryKeyNamesIterable())
					sort[x++] = new SortDirective(key,Order.Ascending);
			}
			if (sort.length>0) {
				StringBuilder ssb = new StringBuilder(" ORDER BY ");
				for (int x=0; x<sort.length; x++) {
					if (x>0) ssb.append(", ");
					ssb.append(sort[x].attribute).append(sort[x].order==Order.Ascending?" ASC":" DESC;");
				}
				ssort=ssb.toString();
			}
			String limitClause = limit==0 ? "" : " LIMIT "+limit;
			
			StringBuilder sb = new StringBuilder("SELECT ").append(selectAttributes).append(" FROM ").append(tablename).append(" WHERE ")
				.append(sfilter).append(ssort).append(limitClause).append(";");

			try (Statement st = ((PostgresqlConnection)store).db.createStatement()) {
				ResultSet rs = st.executeQuery(sb.toString());
				LinkedList<T> result = null;
				if (xform==null)
					result = new LinkedList<T>();
				int count=0;
				while (rs.next()) {
					if (xform==null && ++count>maxTableResultSize)
						throw new U_Exception(U_Exception.ERROR.QueryError,
								String.format("Table adapter result set size %d limit exceeded",maxTableResultSize));
					//We will assume column names have been vetted and are congruent
					@SuppressWarnings("unchecked")
					T tuple = (T)servicedClass.newInstance();
					for (int f=0; f<numberOfAttributes(); f++) {
						// Load the tuple's field from the SQL result set
						tuple.setAttributeValue(f,rs.getObject(RD.attributeNames.get(f)));  // The base Tuple class handles Timestamp to Instant automatically
					}
					try {
						tuple.postLoad((Object[])null);
					} catch (U_Exception e) {
						/* ignore - best effort load */
					}
					if (xform==null)
						result.add(tuple);
					else 
						try {
							if (!xform.action(tuple))
								break;
						} catch (Exception e) {
							if (e.getClass().equals(U_Exception.class))
								throw (U_Exception)e;
							throw new U_Exception(ERROR.Unexpected,e);
						}
				}
				return result;
			} 
			
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.ParserError,"Could not create postgres expression",e);
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"Select failed",e);
		} catch (InstantiationException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,"Unable to create or populate new tuple",e);
		} catch (IllegalAccessException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,"Unable to create or populate new tuple",e);
		}
	}


	/**
	 * Determines if a query returns a non-empty rowset.
	 * @param filter The filter to apply to the query.
	 * @return True if at least one row is returned.
	 * 
	 * @throws U_Exception
	 */
	public boolean isNonEmpty(TupleExpressionFactory.Node filter) throws U_Exception {
		return select(1).size()>0;
	}

	
	/**
	 * Deletes rows from the backing table that match the filter expression.  A null filter matches every row.
	 * 
	 * @param filter The root of an expression tree constructed by an ExpressionFactory<T>.
	 * @return The number of rows deleted.
	 */
	public int delete(TupleExpressionFactory.Node filter) throws U_Exception  {
		try {
			String sfilter = filter==null ? "true" : PostgresqlExpressionGenerator.expand(filter);
			StringBuilder sb = new StringBuilder("DELETE FROM ").append(tablename).append(" WHERE ")
				.append(sfilter).append(";");

			try (Statement st = ((PostgresqlConnection)store).db.createStatement()) {
				st.execute(sb.toString());
				return st.getUpdateCount();
			} 
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.ParserError,"Could not create postgres expression",e);
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"DELETE failed",e);
		} catch (Exception e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,"DELETE failed",e);
		} 
	}
	
	/**
	 * Deletes the tuple t from the backing table if it exists.  The tuple is identified by matching the primary key
	 * values.  Nothing is deleted if there are no primary keys, but no exception is thrown, either.
	 * @param t The tuple to delete.
	 * @return True if the tuple is deleted, or false if it was not found.
	 */
	public boolean delete(T t) throws U_Exception{
		if (numberOfPrimaryKeys()==0) return false;
		try { 
			StringBuilder sb = new StringBuilder("DELETE FROM ").append(tablename).append(" WHERE ")
					.append(PostgresqlExpressionGenerator.expand(getPrimaryKeyExpression(t))).append(";");
			try (Statement st = ((PostgresqlConnection)store).db.createStatement()) {
				st.execute(sb.toString());
				return st.getUpdateCount()!=0;
			}
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,e);
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,e);
		}
	}
	
	/**
	 * Updates the tuple t into the backing table if it exists.  The tuple is identified by matching the primary
	 * key values. Nothing is updated if there are no primary keys, but no exception is thrown, either.
	 * 
	 * @param t The tuple to be updated to the backing table.
	 * @return True if the tuple exists and was updated, else false.
	 */
	public boolean update(T t) throws U_Exception {
		t.preStore((Object[])null);
		if (numberOfPrimaryKeys()==0) return false;
		try { 
			StringBuilder sb = new StringBuilder("UPDATE ").append(tablename).append(" SET ");
			boolean first=true;
			for (int x=0; x<numberOfAttributes(); x++) {
				String attribute = RD.attributeNames.get(x);
				if (isInPrimaryKey(attribute)) continue;   // We don't update key values
				if (!first)
					sb.append(", ");
				sb.append(attribute).append("=?");
				first=false;
			}
			sb.append(" WHERE ").append(PostgresqlExpressionGenerator.expand(getPrimaryKeyExpression(t))).append(";");
			try (PreparedStatement pst = (((PostgresqlConnection)store).db.prepareStatement(sb.toString()))) {
				int parmnum=0;
				for (int x=0; x<numberOfAttributes(); x++) {
					String attribute = RD.attributeNames.get(x);
					if (isInPrimaryKey(attribute)) continue;   // We don't update key values
					setPreparedStatementParameterFromField(pst,t,++parmnum,x);
				}
				pst.execute();
				return pst.getUpdateCount()!=0;
			}
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,e);
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,e);
		}
	}
	
	private void setPreparedStatementParameterFromField(PreparedStatement pst, T t, int parameterQueryPosition, int fieldOffset) throws U_Exception {
		try {
			Object object = t.getAttributeValue(fieldOffset);
			switch (tupleTypeOf(fieldOffset)) {
			case Boolean:
				if (object==null)
					pst.setNull(parameterQueryPosition, java.sql.Types.BOOLEAN);
				else
					pst.setBoolean(parameterQueryPosition, (Boolean)object);
				break;
			case Double:
				if (object==null)
					pst.setNull(parameterQueryPosition, java.sql.Types.DOUBLE);
				else
					pst.setDouble(parameterQueryPosition,  (Double)object);
				break;
			case Instant:
				if (object==null)
					pst.setNull(parameterQueryPosition, java.sql.Types.TIMESTAMP);
				else
					pst.setTimestamp(parameterQueryPosition, Timestamp.from((Instant)object) );
				break;
			case Integer:
				if (object==null)
					pst.setNull(parameterQueryPosition, java.sql.Types.INTEGER);
				else
					pst.setInt(parameterQueryPosition, (Integer)object);
				break;
			case Long:
				if (object==null)
					pst.setNull(parameterQueryPosition, java.sql.Types.BIGINT);
				else
					pst.setLong(parameterQueryPosition, (Long)object);
				break;
			case String:
				if (object==null)
					pst.setNull(parameterQueryPosition, java.sql.Types.VARCHAR);
				pst.setString(parameterQueryPosition, (String)object);
				break;
			default:
				throw new U_Exception(U_Exception.ERROR.Unexpected,"Unsupported type "+tupleTypeOf(fieldOffset).name());
			}
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,e);
		}
	}
	
	/**
	 * Attempts to insert tuple t into the the associated backing table, throwing an
	 * exception if it already exists.  If successful, tuple t is updated with the current
	 * values from the newly inserted row in the table. This allows one to capture default values that may have been
	 * created by the database.
	 */
	public void insert(T t) throws U_Exception {
		insertAux(t,false);
	}
	
	/**
	 * Attempts to insert tuple t into the the associated backing table if it is not
	 * already in the table.  If successful, tuple t is updated with the current
	 * values from the newly inserted row in the table. This allows one to capture default values that may have been
	 * created by the database.
	 */
	public void insertIfNew(T t) throws U_Exception {
		insertAux(t,true);
	}
	
	public void insertAux(T t, boolean onlyIfNew) throws U_Exception {
		t.preStore((Object[])null);
		
		
		try (PreparedStatement pst = ((PostgresqlConnection)store).db.prepareStatement(
				onlyIfNew?INSERT_COMMAND_ON_CONFLICT:INSERT_COMMAND)) {
			for (int parameterIndex=0; parameterIndex<insertAttributesArray.length; parameterIndex++) {
				// We assign to the statement based on types of attributes - this is the safest way, and
				// also helps us account for things like Instant->Timestamp conversion
				setPreparedStatementParameterFromField(pst, t, parameterIndex+1, insertValueOffset[parameterIndex]);
			}
			pst.executeUpdate();
			
			try (PreparedStatement pst2 = db.prepareStatement(SELECT_BY_KEY)) {
				for (int keyIndex=0; keyIndex<whereValueOffset.length; keyIndex++)
					setPreparedStatementParameterFromField(pst2, t, keyIndex+1, whereValueOffset[keyIndex]);
				ResultSet rs = pst2.executeQuery();
				if (!rs.next())
					throw new U_Exception(ERROR.Unexpected,"Could not retrieve inserted row subsequent to insertion");
				//We now reload the ENTIRE tuple with the actual values returned, in case there were default values, etc.
				for (int x=0; x<numberOfAttributes(); x++){
					try {
						t.setAttributeValue(x, rs.getObject(x+1));
					} catch (Exception e) {
						System.out.println(e.getMessage());
					}
				}
			}
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"INSERT failed",e);
		} 
		
	}

	
	
}