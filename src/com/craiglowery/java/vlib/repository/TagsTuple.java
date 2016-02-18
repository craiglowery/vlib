package com.craiglowery.java.vlib.repository;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.tuple.Attribute;
import com.craiglowery.java.vlib.tuple.Ignore;
import com.craiglowery.java.vlib.tuple.PrimaryKey;
import com.craiglowery.java.vlib.tuple.Tuple;
/** 
 * Tuple that maps to the "tags" backing table in the backing store.  It's
 * basically just a single-column list of tag names, but also includes
 * a description, a "type", which is not what you normally would think of
 * as type, but is more like the kind of information the tags represents.
 * The type is not something repository understands. It is to be consumed
 * by clients in helping them understand how to organize and present tagged
 * data. For the reference video repository implementation, the types are
 * Category (tags like Genre, Medium), Entity (tags like Directory, Actor),
 * and Sequence (tags like Season and Episode).
 * 
 * See Tuple for details on the Tuple facility.
 *
 */
public class TagsTuple extends Tuple {
	
	static { try {
		registerSubclass(TagsTuple.class);
	} catch (U_Exception e) {
		throw new RuntimeException(e);
	} }

	public enum TagType {Entity,Category,Sequence};

	@PrimaryKey		public String 	name="";
	@Attribute		public String	description="";
	@Attribute		public String	type="";
	@Attribute		public Integer	browsing_priority=-1;
	@Ignore		    public TagType  type_as_enum=null;
	
	@Override
	public void preStore(Object...parameters) throws U_Exception {
		String t = type==null?"":type.trim().toLowerCase();
		switch (t) {
		case "entity": type="Entity"; break;
		case "category": type="Category"; break;
		case "sequence": type="Sequence"; break;
		default: throw new U_Exception(ERROR.ValidationError,"tag type must be Entity, Category, or Sequence");
		}
	}
	
	@Override
	public void postLoad(Object...parameters) throws U_Exception {
		if (type==null)
			type="Category";
		switch (type) {
		case "Sequence": type_as_enum=TagType.Sequence; break;
		case "Entity": type_as_enum=TagType.Entity; break;
		default: type="Category"; type_as_enum=TagType.Category;
		}
	}
	
}