package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.craiglowery.java.vlib.tuple.Tuple;
import com.craiglowery.java.vlib.tuple.Tuple.Type;

/**
 * 
 * Used to build expression trees for a tuple.  Although this class doesn't use the
 * Java generic typing facility, it still is a generic class, and builds expression
 * trees based on the java class type passed in a construction time.  The reason for
 * this has to do with limitations of Java's type erasure approach to genericity.
 * 
 * The purpose of these expression trees is to enable clients of table adapters
 * to build/specify filter expressions for the table so that they only learn one
 * expression language.  The derived table adapters all share a routine that can
 * reliably translate those expression trees into the expression language of the
 * underlying backing store.
 *
 */
public class TupleExpressionFactory {

	AttributeContext context;
	TupleExpressionFactory factory;
	
	public Node TRUE = null;
	public Node FALSE = null;
	
	public TupleExpressionFactory(Class<? extends Tuple> tupleType)
		throws FilterExpressionException
	{
		context = new AttributeContext(tupleType);
		factory=this;
		TRUE = new LiteralLeafNode(true);
		FALSE = new LiteralLeafNode(false);
	}

	public NotNode not(Node obj)
			throws FilterExpressionException
		{
			return new NotNode(ConvertLiteral(obj));
		}
	
	public OrNode or(Object...args) 
		throws FilterExpressionException
	{
		Node[] nodes = new Node[args.length];
		for (int x=0; x<args.length; x++)
			nodes[x] = ConvertLiteral(args[x]);
		return new OrNode(nodes);
	}
		
	public AndNode and(Object...args) 
			throws FilterExpressionException
	{
		Node[] nodes = new Node[args.length];
		for (int x=0; x<args.length; x++)
			nodes[x] = ConvertLiteral(args[x]);
		return new AndNode(nodes);
	}

	
	public ComparisonNode comp(Object left, Object operator, Object right)
		throws FilterExpressionException
	{
		return new ComparisonNode(ConvertLiteral(left), ConvertOperator(operator), ConvertLiteral(right));
	}
	
	public AttributeLeafNode attribute(String name)
		throws FilterExpressionException
	{
		return new AttributeLeafNode(name);
	}
	
	public LiteralLeafNode litInteger(int i)
		throws FilterExpressionException
	{
		return new LiteralLeafNode(i);
	}
			
	public LiteralLeafNode litLong(long l)
			throws FilterExpressionException
		{
			return new LiteralLeafNode(l);
		}

	public LiteralLeafNode litDouble(double d)
			throws FilterExpressionException
		{
			return new LiteralLeafNode(d);
		}
				
	public LiteralLeafNode litBoolean(boolean b)
			throws FilterExpressionException
		{
			return new LiteralLeafNode(b);
		}
	
	public LiteralLeafNode litString(String s)
			throws FilterExpressionException
		{
			return new LiteralLeafNode(s);
		}
				
	public LiteralLeafNode litInstant(String s)
			throws FilterExpressionException
		{
			try {
				SmartDateTimeParser.parse(s);
			} catch (Exception e) {
				throw new FilterExpressionException(e.getMessage());
			}
			LiteralLeafNode n = new LiteralLeafNode(s);
			n.type = Type.Instant;
			return n;
		}
				

				

	
	public Operator ConvertOperator(Object obj)
		throws FilterExpressionException
	{
		// Probably need to put this in a hash or parser to improve performance.  Parser would be the best route, but
		// would be harder to maintain.
		//
		//I went ahead and did it as a parser instead of cascading if-then-else
		nullCheck(obj);
		if (obj.getClass()==Operator.class)
			return (Operator)obj;
		if (obj.getClass()!=String.class)
			throw new FilterExpressionException("Comparison operator must be an Operator enum or a String representation");
		String s = (String)obj;
		if (s.equals("==") || s.equals("="))
			return Operator.EQUAL_TO;
		if (s.equals("!=") || s.equals("<>"))
			return Operator.NOT_EQUAL_TO;
		if (s.equals(">"))
			return Operator.GREATER_THAN;
		if (s.equals(">="))
			return Operator.GREATER_THAN_OR_EQUAL_TO;
		if (s.equals("<"))
			return Operator.LESS_THAN;
		if (s.equals("<="))
			return Operator.LESS_THAN_OR_EQUAL_TO;
		if (s.equals("$"))
			return Operator.IS_SUBSTRING_OF;
		if (s.equals("~==") || s.equals("~="))
			return Operator.CI_EQUAL_TO;
		if (s.equals("~!=") || s.equals("<>"))
			return Operator.CI_NOT_EQUAL_TO;
		if (s.equals("~>"))
			return Operator.CI_GREATER_THAN;
		if (s.equals("~>="))
			return Operator.CI_GREATER_THAN_OR_EQUAL_TO;
		if (s.equals("~<"))
			return Operator.CI_LESS_THAN;
		if (s.equals("~<="))
			return Operator.CI_LESS_THAN_OR_EQUAL_TO;
		if (s.equals("~$"))
			return Operator.CI_IS_SUBSTRING_OF;
		throw new FilterExpressionException("Unknown comparison operator '%s'",s);
	}
	
	/**
	 * Wraps an object in a node if it isn't already a node.  Performs type checking of obj. If obj is already
	 * assignment compatible with Node, then it is returned cast to Node. Otherwise, the type is used to
	 * guide wrapping the object into a new node.  Type String is treated specially: if the first character is
	 * an at sign (@) and the second character is NOT an at sign (@), then the substring starting at the second
	 * character is taken as an attribute name.  If the string starts with two at signs (@@) then that is
	 * considered an 'escaped' single at sign that does not trigger the attribute name recognition.
	 * @param obj
	 * @return
	 * @throws FilterExpressionException
	 */
	public Node ConvertLiteral(Object obj)
		throws FilterExpressionException
	{
		nullCheck(obj);
		if (Node.class.isInstance(obj))
			return (Node)obj;
		Class<? extends Object> clas = obj.getClass();
		if (clas==Integer.class)
			return new LiteralLeafNode((Integer)obj);
		if (clas==Long.class)
			return new LiteralLeafNode((Long)obj);
		if (clas==Double.class)
			return new LiteralLeafNode((Double)obj);
		if (clas==Boolean.class)
			return new LiteralLeafNode((Boolean)obj);
		if (clas==Instant.class)
			return new LiteralLeafNode((Instant)obj);
		if (clas==String.class) {
			String s = (String)obj;
			if (s.length()>1) {
				//This is a non-empty string of at least two characters in length
				if (s.charAt(0)=='@') {
					//This may be an attribute reference
					if (s.charAt(1)=='@') {
						//this is NOT an attribute reference, but a regular string with escaped @@ marker at the beginning
						//We'll strip the first @ and let it fall through to "regular string" logic below
						s = s.substring(1);
					} else {
						//This is an attribute reference
						String name = s.substring(1);
						return new AttributeLeafNode(name);
					}
				}
			}
			return new LiteralLeafNode(s);
		}
		throw new FilterExpressionException("Unable to automatically convert type '%s' to an expression node",obj.getClass().getName());
		
	}
	
	/**
	 * Returns the type enum of the attribute, or null if the attribute does not exist.
	 * @param name
	 * @return The Type
	 */
	public Type getAttributeType(String name)
		throws FilterExpressionException
	{
		Type t = context.typeMap.get(name);
		if (t==null)
			throw new FilterExpressionException(String.format("Attribute '%s' not known",name));
		return t;
	}
	
	private void nullCheck(Object...args) 
		throws FilterExpressionException 
	{
		for (Object obj : args)
			if (obj==null)
				throw new FilterExpressionException("Null arguments are not permitted for node construction");
	}
	
	
//------------------------------------------------------------------------------------------	
//-- Node  (Inner Class)
//------------------------------------------------------------------------------------------	
	
	public abstract class Node {
	
		public Type type;
		public Kind kind;

		public Node(Type type, Kind kind)
			throws FilterExpressionException
		{
			nullCheck(type);
			nullCheck(kind);
			this.type=type;
			this.kind=kind;
		}	
		
		public AndNode asAnd() { return (AndNode)this; }
		public AttributeLeafNode asAttribute() { return (AttributeLeafNode)this; }
		public ComparisonNode asComparison() { return (ComparisonNode)this; }
		public LiteralLeafNode asLiteral() { return (LiteralLeafNode)this; }
		public NotNode asNot() { return (NotNode)this; }
		public OrNode asOr() { return (OrNode)this; }
		public MultiArgumentNode asMultiArg() { return (MultiArgumentNode)this; }
		
		public boolean isLiteralInstant() {
			return type==Type.Instant && kind==Kind.LITERAL;
		}
		
		public Instant getLiteralInstant()
			throws FilterExpressionException
		{
			if (!isLiteralInstant())
				throw new FilterExpressionException("Attempt to get literal Instant value from unsuitable node");
			return SmartDateTimeParser.tryParse(toString());
		}
		
		public String quoted(String quote) {
			return new StringBuilder(quote).append(toString()).append(quote).toString();
		}
		
		public String escaped(String replace, String with) {
			return toString().replace(replace, with);
		}
		
		public String escapedAndQuoted(String replace, String with, String quote) {
			return new StringBuilder(quote).append(escaped(replace,with)).append(quote).toString();
		}
		
		public abstract void treeView(PrintStream ps, int indent);
		
		public void treeView(PrintStream ps) {
			treeView(ps,0);
		}
		
		void indent(PrintStream ps, int howmuch) {
			while (howmuch-- >0)
				ps.print("   ");
		}

	}

//------------------------------------------------------------------------------------------	
//-- BinaryOperationNode (Inner Class) 	
//------------------------------------------------------------------------------------------
	public abstract class MultiArgumentNode extends Node {
		List<Node> targets = new ArrayList<Node>();

		public MultiArgumentNode(Type type, Kind kind, Node...targs)
			throws FilterExpressionException
		{
			super(type, kind);
			for (Node n : targs) {
				nullCheck(n);
				if (n.type!=Type.Boolean)
					throw new FilterExpressionException("AND/OR can only be applied to BOOLEAN type nodes");
				targets.add(n);
			}
		}
		
		public MultiArgumentNode add(Node additional)
				throws FilterExpressionException
			{
				nullCheck(additional);
				if (additional.type!=Type.Boolean)
					throw new FilterExpressionException("AND/OR can only be applied to BOOLEAN type nodes");
				targets.add(additional);
				return this;
			}

		public void treeView(PrintStream ps, int howmuch) {
			indent(ps,howmuch);
			ps.println(kind==Kind.AND?"AND":"OR");
			for (Node n : targets)
				n.treeView(ps,howmuch+1);
		}
	}
//------------------------------------------------------------------------------------------	
//-- AndNode (Inner Class) 	
//------------------------------------------------------------------------------------------
	public class AndNode extends MultiArgumentNode {
		
		
		public AndNode(Node...nodes) 
			throws FilterExpressionException
		{
			super(Type.Boolean, Kind.AND,nodes);
		}
		
	
	}
	
//------------------------------------------------------------------------------------------	
//-- OrNode (Inner Class)
//------------------------------------------------------------------------------------------
	public class OrNode extends MultiArgumentNode {
		
		List<Node> targets = new ArrayList<Node>();
		
		public OrNode(Node...nodes)
			throws FilterExpressionException
		{
			super(Type.Boolean, Kind.OR, nodes);
		}
		
	}
	
//------------------------------------------------------------------------------------------	
//-- NotNode (Inner Class) 	
//------------------------------------------------------------------------------------------
	public class NotNode extends Node {
		
		public Node target;
		
		public NotNode(Node target)
			throws FilterExpressionException
		{
			super(Type.Boolean, Kind.NOT);
			nullCheck(target);
			if (target.type != Type.Boolean)
				throw new FilterExpressionException("NOT can only be applied to BOOLEAN type nodes");
			this.target = target;
		}
		
		public void treeView(PrintStream ps, int howmuch) {
			indent(ps,howmuch);
			ps.println("NOT");
			target.treeView(ps,howmuch+1);
		}
		
	}
	
//------------------------------------------------------------------------------------------	
//-- LeafNode (Inner Class)
//------------------------------------------------------------------------------------------
	public abstract class LeafNode extends Node {
		
		Object value;
		
		public LeafNode(Object value, Type type, Kind kind)
			throws FilterExpressionException
		{
			super(type,kind);
			nullCheck(value);
			this.value=value;
		}
		
		public String toString() {
			return value.toString();
		}
		
		
	}
	
//------------------------------------------------------------------------------------------	
//-- LiteralLeafNode
//------------------------------------------------------------------------------------------
	public class LiteralLeafNode extends LeafNode {
		
		public LiteralLeafNode(Integer value)
			throws FilterExpressionException
		{
			super(value,Type.Integer,Kind.LITERAL);
		}
		
		public LiteralLeafNode(Long value)
			throws FilterExpressionException
		{
			super(value,Type.Long,Kind.LITERAL);
		}
		
		public LiteralLeafNode(Double value)
			throws FilterExpressionException
		{
			super(value,Type.Double,Kind.LITERAL);
		}
		
		public LiteralLeafNode(String value) 
			throws FilterExpressionException
		{
			super(value,Type.String,Kind.LITERAL);
		}
		
		public LiteralLeafNode(Instant value)
			throws FilterExpressionException
		{
			super(value,Type.Instant,Kind.LITERAL);
		}
		
		public LiteralLeafNode(Boolean value) 
			throws FilterExpressionException
		{
			super(value,Type.Boolean,Kind.LITERAL);
		}
		
		public void treeView(PrintStream ps, int howmuch) {
			indent(ps,howmuch);
			ps.println(type==Type.String?escapedAndQuoted("'", "''", "'"):toString());
		}
		
	}
	
//------------------------------------------------------------------------------------------	
//-- AttributeLeafNode (Inner Class)
//------------------------------------------------------------------------------------------	
	public class AttributeLeafNode extends LeafNode {
		
		public AttributeLeafNode(String attributeName)
			throws FilterExpressionException
		{
			super(attributeName,getAttributeType(attributeName),Kind.ATTRIBUTE);
		}
		
		public void treeView(PrintStream ps, int howmuch) {
			indent(ps,howmuch);
			ps.println("@"+toString());
		}
		
	}

//------------------------------------------------------------------------------------------	
//-- ComparisonNode (Inner Class)
//------------------------------------------------------------------------------------------	
	public class ComparisonNode extends Node

	{
		Node left, right;
		Operator op;
	
		public ComparisonNode(Node left, Operator op, Node right)
			throws FilterExpressionException
		{
			super(Type.Boolean,Kind.COMPARISON);
			nullCheck(left,op,right);
			
			/* We have to type check the operators against arguments
			 * 
			 *       ARGTYPE   ==  !=  <=  >=   <   >   $  ~== ~!= ~<= ~>= ~<  ~>  ~$
			 *       --------- --- --- --- --- --- --- --- --- --- --- --- --- --- ---
			 *     1 INTEGER    X   X   X   X   X   X   X
			 *     1 LONG       X   X   X   X   X   X   X
			 *     1 DOUBLE     X   X   X   X   X   X   X
			 *     2 BOOLEAN    X   X   X   X   X   X   X
			 *     4 STRING     X   X   X   X   X   X   X   X   X   X   X   X   X   X 
			 *     8 INSTANT    X   X   X   X   X   X   X
			 *                 
			 *
			 *  We could consider the fact that some persistence layers may allow type promotion,
			 *  such as INTEGER to LONG, or either of those to BOOLEAN. We are going to only support a few common
			 *  cases, but otherwise be pretty strict.
			 *   
			 *   INTEGER, LONG and DOUBLE will be considered compatible for comparison as "numbers"
			 *   
			 *   If one side of a comparison is INSTANT, the other side can be INSTANT or STRING. If it is STRING
			 *   it must pass parsing for INSTANT
			 *      
			 */

			if (left.type==Type.Instant) {
				if (right.type==Type.String && SmartDateTimeParser.tryParse(right.toString())!=null)
					right.type=Type.Instant;
				else if (right.type!=Type.Instant)
					throw new FilterExpressionException("INSTANT can only be compared to other INSTANTs or SmartDateTime-parsable STRING");
			} else if (right.type==Type.Instant){
				if (left.type==Type.String && SmartDateTimeParser.tryParse(left.toString())!=null)
					left.type=Type.Instant;
				else if (left.type!=Type.Instant)
					throw new FilterExpressionException("INSTANT can only be compared to other INSTANTs or SmartDateTime-parsable STRING");
			}
			
			if (classifyType(left)!=classifyType(right))
				throw new FilterExpressionException("Comparison operations must be performed on simlar-type operands");
			
			if (op.ordinal() >= Operator.IS_SUBSTRING_OF.ordinal() && left.type!=Type.String)
				throw new FilterExpressionException("Substring and case-insensitive comparisons can only be performed on strings");
			
			this.left=left;
			this.right=right;
			this.op=op;
			
		}
		
		public void treeView(PrintStream ps, int howmuch) {
			indent(ps,howmuch);
			ps.println(op.toString());
			left.treeView(ps,howmuch+1);
			right.treeView(ps,howmuch+1);
		}
		
	}
	
	private int classifyType(Node node) {
		int c = node.type.ordinal();
		return (c<=Type.Double.ordinal()) ? 1 : c;
	}
	
	public static Type classifyJavaType(Class<? extends Object> clazz) {
		if (clazz==int.class || clazz==Integer.class) return Type.Integer;
		if (clazz==double.class || clazz==Double.class) return Type.Double;
		if (clazz==long.class || clazz==Long.class) return Type.Long;
		if (clazz==String.class ) return Type.String;
		if (clazz==boolean.class || clazz==Boolean.class) return Type.Boolean;
		if (clazz==Instant.class) return Type.Instant;
		return null;
		
	}
	
	public Node parse(String expression) throws FilterExpressionException {
		StringExpressionParser p =null;
		List<String> errorList = null;
		try {
			errorList = new LinkedList<String>();
			p = new StringExpressionParser(expression,this,errorList);

			return (TupleExpressionFactory.Node)(p.parse().value);
		} catch (Exception e) {
			throw new FilterExpressionException(e.getMessage());
		}
	}

}
	
