package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.craiglowery.java.vlib.tuple.Attribute;
import com.craiglowery.java.vlib.tuple.PrimaryKey;
import com.craiglowery.java.vlib.tuple.Tuple;
import com.craiglowery.java.vlib.tuple.Tuple.Type;

/**
 * Used by the filter node framework to validate attribute names and types.
 *
 * @param <T> A subclass of Tuple, which has been annotated with @Attribute and
 *            @PrimaryKey markers on the fields which should be treated as
 *            attributes.
 */
public class AttributeContext {
	
	/** Use to determine if a field name exists, and its Type */
	public Map<String,Type> typeMap = new HashMap<String,Type>();
	/** Use to determine if a field is a primary key */
	public List<String> primaryKeys = new LinkedList<String>();
	/** A record of the tuple class used to build this context */
	public Class<? extends Tuple> tupleClass;
	
	public AttributeContext(Class<? extends Tuple> tupleClass)
		throws FilterExpressionException
	{
		if (tupleClass==null)
			throw new FilterExpressionException("AttributeContext must be provided a non-null type reference");
		this.tupleClass = tupleClass;
		for (Field field : tupleClass.getDeclaredFields()) {
			if (field.isAnnotationPresent(PrimaryKey.class))
				primaryKeys.add(addToMap(field));
			 else if (field.isAnnotationPresent(Attribute.class))
				addToMap(field);
		}
	}
	
	private String addToMap(Field f)
		throws FilterExpressionException
	{
		
		String name = f.getName();
		Type type = null;
		Class<? extends Object> clas = f.getType();
		if (clas==Integer.class || clas==int.class)
			type = Type.Integer;
		else if (clas==Long.class || clas==long.class)
			type = Type.Long;
		else if (clas==Double.class || clas==double.class)
			type = Type.Double;
		else if (clas==String.class)
			type = Type.String;
		else if (clas==Boolean.class || clas==boolean.class)
			type = Type.Boolean;
		else if (clas==Instant.class)
			type = Type.Instant;
		else
			throw new FilterExpressionException(
					"Field '%s' of tuple class '%s' is type '%s' which is not supported as an @Attribute",
					name, tupleClass.getName(), clas.getName());
		typeMap.put(name,type);
		return name;
	}
}
