package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.tuple.Tuple.Type;

/**
 * This class supports Tuple and TableAdapter classes in efficiently accessing
 * the reflection data for a tuple, such as which fields have specific annotations.
 * One of the key values of this class is that it interrogates the reflection system
 * only once and caches the results for faster access in the future.
 *
 */
public class TupleSubClassReflectedData {

	protected  ArrayList<Field>    fields = new ArrayList<Field>();
	protected  ArrayList<Type>     tupleTypes = new ArrayList<Tuple.Type>();
	protected  ArrayList<Class<?>> javaTypes = new ArrayList<Class<?>>();
	protected  ArrayList<String>   attributeNames = new ArrayList<String>();
	protected  Map<String,Integer> primaryKeysIndex = new HashMap<String,Integer>();
	protected  Map<String,Integer> attributesIndex = new HashMap<String,Integer>();
	protected  Map<String,Integer> defaultOnInsertIndex = new HashMap<String, Integer>();
	
	private  int numberOfAttributes=0;
	
	public TupleSubClassReflectedData(Class<? extends Tuple> clazz) throws U_Exception 
	{
		for (Field field : clazz.getDeclaredFields()) {
			if (field.isAnnotationPresent(Ignore.class))  //Ignore fields annotated with @Ignore
				continue;
			boolean isMarkedAttribute = field.isAnnotationPresent(Attribute.class);
			boolean isMarkedPrimaryKey = field.isAnnotationPresent(PrimaryKey.class);
			if (isMarkedAttribute || isMarkedPrimaryKey) {
				String name = field.getName();
				fields.add(field);
				attributesIndex.put(name, numberOfAttributes);
				attributeNames.add(name);
				if (isMarkedPrimaryKey)
					primaryKeysIndex.put(name, numberOfAttributes);					
				Class<?> javaType = field.getType();
				javaTypes.add(javaType);
				Type tupleType=null;
				if (javaType==String.class)
					tupleType=Type.String;
				else if (javaType==Integer.class) 
					tupleType=Type.Integer;
				else if (javaType==Long.class) 
					tupleType=Type.Long;
				else if (javaType==Double.class)
					tupleType= Type.Double;
				else if (javaType==Boolean.class)
					tupleType= Type.Boolean;
				else if (javaType==Instant.class)
					tupleType = Type.Instant;
				else {
					tupleType = Type.Unknown;
					throw new U_Exception(ERROR.ConfigurationError,"Unsupported java type "+javaType.getName()+" for field "+name);
				}
				tupleTypes.add(tupleType);
				if (field.isAnnotationPresent(DefaultOnInsert.class))
					defaultOnInsertIndex.put(name, numberOfAttributes);
				numberOfAttributes++;
			}
		}
	}

}