package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import com.craiglowery.java.vlib.common.U_Exception;


/**
 * An abstract class from which table-specific tuple classes are derived.  A Tuple is like the row of a table.
 * It knows what attributes are from the table, which attributes are keys, and their datatypes.  It works with a
 * TableAdapter object to perform operations such as select, update, and delete in a backend persistence store.<p>
 * 
 * Only one level of subclassing is supported. (In other words, only direct children of Tuple.)  Subclasses
 * further derived, like grandchildren, are not supported.<p>
 * 
 * When subclassing, one puts the attributes of the tuple as fields in the subclass public space, marking them
 * with the @Attribute annotation from this package. Fields that participate in the primary key should be
 * marked with the @PrimaryKey annotation. (The @Attribute annotation is redundant in this case.) Only the 
 * following types are supported:<p>
 * 
 * <ol>
 *    <li> String
 *    <li> Integer
 *    <li> Long
 *    <li> Double
 *    <li> Boolean
 *    <li> Instant
 * </ol>
 * 
 * A sub-class should call the registerSubclass() method passing its own class object in
 * order to support reflection caching. For example:<p>
 * 
 * <pre>
 *   ObjectsTuple.registerSubclass(ObjectsTuple.class);
 * </pre>
 * 
 * @author James Craig Lowery 
 *
 */
public abstract class Tuple {

	public static enum Type {Unknown,String,Integer,Long,Double,Boolean,Instant};
	
	private static Map<Class<? extends Tuple>,TupleSubClassReflectedData> reflectedData =
			new HashMap<Class<? extends Tuple>, TupleSubClassReflectedData>();

	protected static void registerSubclass(Class<? extends Tuple> clazz) throws U_Exception {
		if (!reflectedData.containsKey(clazz))
			reflectedData.put(clazz, new TupleSubClassReflectedData(clazz));
	}

	public static TupleSubClassReflectedData getReflectedData(Class<? extends Tuple> clazz) {
		return reflectedData.get(clazz);
	}

	public TupleSubClassReflectedData getReflectedData() {
		return reflectedData.get(this.getClass());
	}
	

	public Tuple() {
	} 
	
	
	//------------------------------------------------------------------------------------------	
	//-- Attribute setters
	//------------------------------------------------------------------------------------------	
		/**
		 * Set's a field's value, assuming it is type compatible.
		 * @param field The integer key  of the field to be set.
		 * @param value The value to be set, which must be type-compatible with the field.
		 * @throws U_Exception
		 */
		public void setAttributeValue(Integer fieldIndex, Object value) throws U_Exception {
			TupleSubClassReflectedData rd = getReflectedData();
			if (value!=null) {
				Type targetType = rd.tupleTypes.get(fieldIndex);
				//We provide jdbc Timestamp conversion built-in as a courtesy
				if (targetType==Type.Instant && value.getClass()==java.sql.Timestamp.class) {
					value = ((java.sql.Timestamp)value).toInstant();
				} else {
					Type sourceType = classifyJavaType(value.getClass());
					if (sourceType!=targetType) {
						if (sourceType==Type.Integer && targetType==Type.Long)
							value = new Long((Integer)value);
						else
							throw new U_Exception(U_Exception.ERROR.TypeMismatch,
								String.format("Field '%s' expects type '%s' not '%s'",
									rd.attributeNames.get(fieldIndex),
									targetType.name(),sourceType.name()));
					}
				}
			}
			try {
				rd.fields.get(fieldIndex).set(this, value);
			} catch (Exception e) {
				throw new U_Exception(U_Exception.ERROR.Unexpected,"Could not set value on field "+
						getReflectedData().attributeNames.get(fieldIndex),e);
			}
		}
		
		public void setAttributeValue(String fieldname, Object value) throws U_Exception {
			setAttributeValue(getReflectedData().attributesIndex.get(fieldname),value);
		}
		
		
	//------------------------------------------------------------------------------------------	
	//-- Attribute getters
	//------------------------------------------------------------------------------------------	
		
		//-- Object
		
		/**
		 * Get's a field's value as Object, using the Field object directly.
		 * @param field The field
		 * @return The value of the field
		 * @throws U_Exception
		 */
		public Object getAttributeValue(Integer fieldIndex) throws U_Exception {
			try {
				return getReflectedData().fields.get(fieldIndex).get(this);
			} catch (Exception e) {
				throw new U_Exception(U_Exception.ERROR.Unexpected,"Getting value of field at position "+
					fieldIndex);
			}
		}
		
		/**
		 * Get's a field's value as Object, using the field name for lookup.
		 * @param fieldname The name of the field to lookup and get the value for.
		 * @return The object value of the field.
		 * @throws U_Exception
		 */
		public Object getAttributeValue(String fieldname) throws U_Exception {
			TupleSubClassReflectedData rd = getReflectedData();
			try{
				return rd.fields.get(rd.attributesIndex.get(fieldname)).get(this);
			} catch (Exception e) {
				throw new U_Exception(U_Exception.ERROR.Unexpected,"Getting value of field "+
					fieldname);
			}
		}
		
		private static Map<Class<?>,Type> classMap = new HashMap<Class<?>,Type>();
		{
			classMap.put(Integer.class,Type.Integer);
			classMap.put(Long.class,Type.Long);
			classMap.put(Double.class,Type.Double);
			classMap.put(String.class,Type.String);
			classMap.put(Instant.class, Type.Instant);
			classMap.put(Boolean.class, Type.Boolean);
		}
		
		public static Type classifyJavaType(Class<?> clazz) {
			return classMap.get(clazz);
		}

		public Type tupleTypeOf(int fieldIndex) {
			return getReflectedData().tupleTypes.get(fieldIndex);
		}
		
		public Type tupleTypeOf(String fieldName) {
			TupleSubClassReflectedData rd = getReflectedData();
			return rd.tupleTypes.get(rd.attributesIndex.get(fieldName));
		}
		
		public Class<?> javaTypeOf(int fieldIndex) {
			return getReflectedData().javaTypes.get(fieldIndex);
		}
		
		public Class<?> javaTypeOf(String fieldName) {
			TupleSubClassReflectedData rd = getReflectedData();
			return rd.javaTypes.get(rd.attributesIndex.get(fieldName));
		}
		
		/**
		 * Called by table adapters after they load a tuple to perform
		 * an post-load processing.
		 * @param parameters Optional parameters, as deemed necessary by
		 * the designer of the derived class.
		 * @throws {@code U_Exception} with an {@code ERROR.ValidationError}
		 */
		public void postLoad(Object...parameters)  throws U_Exception {
		}
		
		/**
		 * Called by table adapters before they store a tuple to
		 * perform any pre-storage processing. 
		 * @param parameters Optional parameters, as deemed necessary by
		 * the designer of the derived class.
		 * @throws {@code U_Exception} with an {@code ERROR.ValidationError}
		 * if processing fails.
		 */
		public void preStore(Object...parameters) throws U_Exception {
		}
	
}




