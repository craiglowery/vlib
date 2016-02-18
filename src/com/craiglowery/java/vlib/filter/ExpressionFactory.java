package com.craiglowery.java.vlib.filter;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */


import java.time.Instant;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.repository.RepositoryManager;
import com.craiglowery.java.vlib.repository.TagsTuple;
import com.craiglowery.java.vlib.repository.VersionsTuple;
import com.craiglowery.java.vlib.tuple.TableAdapter;
import com.craiglowery.java.vlib.tuple.Tuple;
import com.craiglowery.java.vlib.tuple.filterexp.SmartDateTimeParser;

/**
 * Used to create expression trees of the repository's attribute and tagging
 * facility to create filters.
 *
 */
public class ExpressionFactory {

	public TableAdapter<VersionsTuple> ta;
	public enum Type {Unknown, String, Integer, Double, Boolean, TimeStamp, Tag };
	private RepositoryManager rm;
	public enum Operator
	{
		Equals,
		CI_Equals,
		DoesNotEqual,
		CI_DoesNotEqual,
		LessThan,
		CI_LessThan,
		LessThanOrEqualTo,
		CI_LessThanOrEqualTo,
		GreaterThan,
		CI_GreaterThan,
		GreaterThanOrEqualTo,
		CI_GreaterThanOrEqualTo,
		Includes,
		CI_Includes,
		DoesNotInclude,
		CI_DoesNotInclude,
		IsASubstringOf,
		CI_IsASubstringOf,
		BooleanAnd,
		BooleanOr,
		BooleanNot
	};

	private HashSet<String> tagNames = new HashSet<String>();
	
	private int handleFieldOffset=-1;
	
	public ExpressionFactory(RepositoryManager rm, TableAdapter<VersionsTuple> ta) throws ExprException {
		this.ta=ta;
		this.rm=rm;
		tagNames = new HashSet<String>(50);
		try {
			for (TagsTuple tt : rm.getTags())
				tagNames.add(tt.name.toLowerCase());
		} catch (U_Exception e) {
			throw new ExprException("Error retrieving tag names",e);
		}
		handleFieldOffset = ta.attributeOffset("handle");
		if ( ta.javaTypeOf(handleFieldOffset)!=Integer.class)
			throw new ExprException("The table must have a 'handle' field of type Integer in order to support tagging");
	}


	/**
	 * Creates a new instance of a derived class of AttributeExpr that is
	 * appropriate for the attribute named.
	 * @param name The name of the attribute.
	 * @return
	 */
	public AttributeExpr createAttribute(String name) throws ExprException {
		Tuple.Type underlyingType = ta.tupleTypeOf(name);
		if (underlyingType!=null) {
			//It's an attribute with a non-tag type
			switch (underlyingType) {
			case Boolean: return new BooleanAttributeExpr(name);
			case Double: return new DoubleAttributeExpr(name);
			case Instant: return new TimeStampAttributeExpr(name);
			case Integer: return new IntegerAttributeExpr(name,true);
			case Long: return new IntegerAttributeExpr(name,false);
			case String: return new StringAttributeExpr(name);
			default:
			}
		} else {
			//It is either a tag name, or an unknown name
			if (tagNames.contains(name.toLowerCase()))
				return new TagAttributeExpr(name);
		}
		throw new ExprException("Unknown or unsupported attribute name: "+name);
	}

	/** Creates a new literal based on a string expression and using
	 * automatic type detection.<p>
	 * 
	 * Auto-typing follows the steps below:<p>
	 * <ol>
	 *    <li> Try to parse as an integer.
	 *    <li> Try to parse as a double.
	 *    <li> Try to parse as a timestamp.
	 *    <li> Try to parse as a boolean.
	 *    <li> Try to parse as a tag, using the format "[red,green,blue]"
	 *    <li> Type as String
	 * </ol>
	 **/
	public LiteralExpr createLiteral(String s) {

		Long l = parseInteger(s);
		if (l!=null) return new IntegerLiteralExpr(l);
		
		Double d = parseDouble(s);
		if (d!=null) return new DoubleLiteralExpr(d);
		
		TimeStampType t = parseTimeStamp(s);
		if (t!=null) return new TimeStampLiteralExpr(t);
		
		Boolean b = parseBoolean(s);
		if (b!=null) return new BooleanLiteralExpr(b);

		TagType g = parseTag(s);
		if (g!=null) return new TagLiteralExpr(g);
		
		return new StringLiteralExpr(s);
	}
	
	public Expr createBinaryOperation(Expr left,ExpressionFactory.Operator operator, Expr right) 
		throws ExprException {
		switch (left.type) {
		case Boolean:
			//If left is Boolean, then right must be boolean as well
			if (right.type==Type.Boolean)
				return new BooleanBinaryExpr(left, operator, right);
			break;
		case Double: 
			//If left is Double, then right can be Double or Integer
			if (right.type==Type.Double || right.type==Type.Integer)
				return new DoubleBinaryExpr(left, operator,right);
			break;
		case Integer: 
			//If left is Integer, then right can be Integer or Doubler
			if (right.type==Type.Integer)
				return new IntegerBinaryExpr(left, operator, right);
			if (right.type==Type.Double)
				return new DoubleBinaryExpr(left,operator,right);
			break;
		case String:
			//If left is a String, then it may be interpreted as String or Timestamp
			//Which of these we will apply depends on the type of the right
			if (right.type==Type.String)
				return new StringBinaryExpr(left,operator,right);
			if (right.type==Type.TimeStamp)
				return new TimeStampBinaryExpr(left,operator,right);
			break;
		case Tag:
			//If left is a Tag, then right may be Tag or String
			if (right.type==Type.Tag || right.type==Type.String)
				return new TagBinaryExpr(left,operator,right);
		case TimeStamp:
			// If left is a TimeStamp, then right may either be another TimeStamp or a
			// String to be interpreted as a timestamp
			if (right.type==Type.TimeStamp || right.type==Type.String)
				return new TimeStampBinaryExpr(left,operator,right);	
		default:
		}
		throw new ExprException("Unknown left-hand type for binary operation: "+left.type.name());
	}

	
	public Expr createUnaryOperation(ExpressionFactory.Operator operator, Expr operand) 
			throws ExprException {
			switch (operand.type) {
			case Boolean:
				if (operator==Operator.BooleanNot)
					return new BooleanNotExpr(operand);
				break;
			default:
			}
			throw new ExprException(String.format("Unsupported unary operation %s %s", 
					operator.name(),operand.type.name()));
		}

	public TagType parseTag(String s) {
		if (s.startsWith("[") && s.endsWith("]")) {
			Matcher m = literalTagParsePattern.matcher(s.substring(1));
			TreeSet<String> litset = new TreeSet<String>();
			while (m.find()) {
				litset.add(m.group(1));
			}
			if (m.hitEnd())
				return new TagType(litset);
		}
		return null;
	}
	private Pattern literalTagParsePattern = Pattern.compile("\\s*([^,\\]]+?)\\s*(?:,|])");

	public Long parseInteger(String s) {
		try {
			return Long.parseLong(s);
		} catch (NumberFormatException e) {}
		return null;
	}
	
	public Double parseDouble(String s) {
		try {
			return Double.parseDouble(s);
		} catch (NumberFormatException e) {}
		return null;
	}
	
	public TimeStampType parseTimeStamp(String s) {
		try {
			Instant i = SmartDateTimeParser.parse(s);
			return new TimeStampType(i);
		} catch (NumberFormatException e) {}
		return null;
	}
	
	public Boolean parseBoolean(String s) {
		if (s.equalsIgnoreCase("TRUE"))
			return Boolean.TRUE;
		if (s.equalsIgnoreCase("FALSE"))
			return Boolean.FALSE;
		return null;
	}
	
	/** Creates a new literal based on a string expression and a type.
	 * The string expression must parse as the type.
	 * @param s The string to parse.
	 * @param typ The type to which s must parse.
	 * @return A new instance of the proper subclass of literalExpr.
	 */
	public LiteralExpr createLiteral(String s, Type typ) throws ExprException {
		switch (typ) {
			case Boolean:
				Boolean b = parseBoolean(s);
				if (b!=null) return new BooleanLiteralExpr(b);
				break;
			case Double:
				Double d = parseDouble(s);
				if (s!=null) return new DoubleLiteralExpr(d);
				break;
			case Integer:
				Long l = parseInteger(s);
				if (l!=null) return new IntegerLiteralExpr(l);
				break;
			case String:
				if (s!=null) return new StringLiteralExpr(s);
				break;
			case Tag:
				TagType g = parseTag(s);
				if (g!=null) return new TagLiteralExpr(g);
				break;
			case TimeStamp:
				TimeStampType t = parseTimeStamp(s);
				if (t!=null) return new TimeStampLiteralExpr(t);
				break;
			default:
		}
		throw new ExprException(String.format("String '%s' did not parse as %s",
				s==null?"(null)":s,typ.name()));
	}
	

	public Dictionary<String,Type> getAttributeDescriptors() {
		Dictionary<String,Type> des = new Hashtable<String, Type>();
		for (String s : ta.attributeNamesIterable()) {
			des.put(s,mapTupleAdapterType(ta.tupleTypeOf(s)));
		}
		//Go through the tags
		for (String s : this.tagNames) {
			des.put(s, Type.Tag);
		}
		return des;
	}
	
	public Type mapTupleAdapterType(Tuple.Type t) {
		switch (t) {
		case Boolean: return Type.Boolean;
		case Double: return Type.Double;
		case Instant: return Type.TimeStamp;
		case Integer: return Type.Integer;
		case Long: return Type.Integer;
		case String: return Type.String;
		default: return Type.Unknown;
		}
	}
	
//------------------------------------------------------------------------------------------	
//-- Expr 	
//------------------------------------------------------------------------------------------	
	
	/**
	 * Abstract base class for the expression hierarchy. All expressions must have a known type.  Leaf-node expressions,
	 * which are subclassed from this class, are either attributes (identifiers) of an object, or a literal of a
	 * supported type.  The supported types are:<p>
	 * 
	 * <dl>
	 *   <dt>string	<dd>a sequence of characters.  Literal strings can be delimited with ' or "
	 *        
	 *   <dt>integer <dd>a 64-bit signed value (i.e., long). Literal integers are written in the 
	 *   			conventional way.
	 *        
	 *   <dt>bool    <dd>logical TRUE and FALSE. Literal booleans are the tokens TRUE or FALSE.
	 *        
	 *   <dt>double	 <dd>a double precision floating point. Literals are identified by the presence of a radix point.
	 *        
	 *   <dt>tag 	 <dd>Think of the tag type as being a set of strings.  For a particular object, an attribute name
	 *                    that is really a tag name evaluates to a set of strings for which expressions exist to test membership.   
	 *                    For example, the tag name Color may be the set 'Yellow', 'Orange' and 'Blue.'  There are operators to test
	 *                    for membership, such as Color='Yellow'  (Color includes 'Yellow').  A tag literal (a set of strings)
	 *                    is written as [string, string, string] where each string in the list is a string literal.
	 *        
	 *   <dt>timestamp	<dd>a fixed point in time.  Timestamp literals are written in enclosing braces {}. The format is very liberal
	 *        				and the parser can recognize most unambiguous formats.  
	 * </dl>
	 * Operations on types include:<p>
	 * 
	 * <dl>
	 *  <dt>Comparison (=,!=,&lt;,&gt;,&lt;=,&gt;=)	
	 *  		<dd>For all types except tag (see below), these operate as expected.  For strings, prepending
	 *        								the operator with a tilde (~) makes the comparison case insensitive.  For example, ~= performs
	 *        								a case-insensitive equality comparison.  <p>
	 *        
	 *        								If both operands are of type tag, then the 
	 *        								comparison is A and B are the same set, A and B differ, A is a proper subset of B, 
	 *        								A is a proper superset of B, A is a subset of B, and A is a superset of B.<p>
	 *        
	 *        								If one operand is type tag and the other operand is type string then
	 *        								only the following comparisons are allowed:<p>
	 *        
	 *        								<dl>
	 *         								<dt>tag = string 	<dd> tag includes string
	 *         								<dt>tag != string	 <dd> tag does not include string
	 *         								<dt>tag ~= string 	<dd> tag includes string (case insensitive)
	 *         								<dt>tag ~!= string	 <dd> tag does not include string (case insensitive)
	 *        								</dl>
	 *        								To test whether a tag is or is not defined for an object, compare against the empty		
	 *        								set as in<p>
	 *        								<dl>
	 *        											<dt>Color=[] <dd>true if there is no color defined for this object
	 *        											<dt>Color!=[] <dd>true if there is at least one color defined for this object
	 *        								</dl>
	 *        <dt>
	 *        Substring  ($)	<dd>			True if left is a substring of right.  Prepending with a tilde (~$) causes the substring
	 *        								search to be performed without case sensitivity.  Can only be used with string operands.
	 *        
	 *        <dt>
	 *        Logical (AND, OR)	<dd> Implements logical conjunction and disjunction in the conventional manner.
	 * </dl>    
	 *   
	 * Attribute names are the typical variable-type names found in languages like Java and C.  The type is determined automatically.<p>
	 * Expressions are combined by the UnaryExpr and BinaryExpr
	 * subclasses, which test compatibility between types of left and
	 * right expressions, and the operator.  <p>
	 * 
	 * The hierarchy is:<p>
	 * <ul>
	 *    <li>Expr (abs)
	 *    <ul>
	 *       <li>LeafExpr  (abs)
	 *       <ul>
	 *          <li>LiteralExpr  (abs)
	 *          <ul>
	 *             <li>StringLiteralExpr
	 *             <li>BooleanLiteralExpr
	 *             <li>IntegerLiteralExpr
	 *             <li>DoubleLiteralExpr
	 *             <li>TimeStampLiteralExpr
	 *             <li>TagLiteralExpr
	 *          </ul>
	 *          <li>AttributeExpr (abs)
	 *          <ul>
	 *            <li>StringAttributeExpr
	 *            <li>BooleanAttributeExpr
	 *            <li>IntegerAttributeExpr
	 *            <li>DoubleAttributeExpr
	 *            <li>TimeStampAttributeExpr
	 *            <li>TagAttributeExpr
	 *          </ul>
	 *       </ul>
	 *       <li>UnaryExpr (abs)
	 *       <ul>
	 *         <li>BooleanNotExpr
	 *       </ul>
	 *       <li>BinaryExpr (abs)
	 *       <ul>
	 *         <li>StringBinaryExpr
	 *         <li>BooleanBinaryExpr
	 *         <li>IntegerBinaryExpr
	 *         <li>DoubleBinaryExpr
	 *         <li>TimeStampBinaryExpr
	 *         <li>TagBinaryExpr
	 *       </ul>
	 *    </ul>
	 * </ul>
	 * 
	 * 
	 * 
	 * After an expression tree is built, the root Expr node is used to evaluate the tree against
	 * a VersionTuple instance.  The Result inner class is used to transmit a value/type pairing
	 * which is the result of the evaluation. 
	 * In most cases, the root node is expected to return a boolean typed
	 * value indicating whether or not the tuple should pass through a filter.<p>
	 */
	public abstract class Expr {
		
		public Type type=Type.Unknown;
		
		public boolean constantFlag=false;
		public Object value=null;
		@SuppressWarnings("unused")
		private boolean isRoot=false;
		private boolean isPrepped=false;
		
		/**
		 * Constructs a new expression node with the given type.
		 * @param type The type of the node.
		 */
		public Expr(Type type) {
			this.type = type;
		}
		
		public Expr() {
		}
		
		/**
		 * Should be called as the last thing in an Expr subclass constructor to
		 * compute and store constant values for future short-circuiting.
		 * @throws ExprException
		 */
		protected void prewireConstant() throws ExprException {
			if (constantFlag) {
				constantFlag=false;
				subordinateEval(null);
				constantFlag=true;
			}

		}
		
		public boolean isConstant() {
			return constantFlag;
		}
		
		/**
		 * This method should be called on the root note of an expression tree to 
		 * evaluate the tree.  This is how the root node knows it is the root node
		 * and can do additional housekeeping to prepare the evaluation environment
		 * if necessary.
		 * @param vt
		 * @return
		 */
		public Result eval(Tuple vt) throws ExprException {
			isRoot=true;
			if (!isPrepped) {
				try {
					if (usesTagType())
						rm.tm_freshenCache();
				} catch (U_Exception e) {
					throw new ExprException("Could not prepare tag membership testing cache",e);
				}
				isPrepped=true;
			}
			return subordinateEval(vt);
		}
		
		public abstract Result subordinateEval(Tuple vt) throws ExprException;
		public boolean usesTagType() {
			return false;
		}
		
	
		public boolean isLeaf() { return false; }
		public boolean isLiteral() { return false; }
		public boolean isAttribute() { return false; }
		
		public abstract Node xml(Document doc);
	
		public Double getDouble() throws ExprException {
			if (value==null)
				return 0.0;
			if (type==Type.Integer)	
				return 0.0 + (Long)value;
			if (type==Type.Double)
				return (Double)value;
			throw new ExprException(String.format("Type %s is not Double compatible", type.name()));
		}

		public Long getInteger() throws ExprException {
			if (value==null)
				return 0L;
			if (type==Type.Integer)
				return (Long)value;
			throw new ExprException(String.format("Type %s is not Integer compatible", type.name()));
		}

		public Boolean getBoolean() throws ExprException {
			if (value==null)
				return false;
			if (type==Type.Boolean)
				return (Boolean)value;
			throw new ExprException(String.format("Type %s is not Boolean compatible", type.name()));
		}
		
		public String getString() throws ExprException {
			if (value==null)
				return "";
			return value.toString();
		}
		
		public TagType getTag() throws ExprException {
			if (value==null)
				return new TagType();
			if (type==Type.Tag)
				return (TagType)value;
			throw new ExprException(String.format("Type %s is not Tag compatible", type.name()));
		}

		public TimeStampType getTimeStamp() throws ExprException {
			if (value==null)
				return new TimeStampType(Instant.ofEpochSecond(0L));;
			if (type==Type.TimeStamp)
				return (TimeStampType)value;
			throw new ExprException(String.format("Type %s is not TimeStamp compatible", type.name()));
		}

	}
	
	public final BooleanLiteralExpr TRUE = new BooleanLiteralExpr(true);
	public final BooleanLiteralExpr FALSE = new BooleanLiteralExpr(false);
	public final Result RESULT_TRUE = new Result(TRUE,Type.Boolean);
	public final Result RESULT_FALSE = new Result(FALSE, Type.Boolean);
	
	public class Result {
		public Object object;
		public Type type;
		public Result(Object object, Type type) {
			this.object=object;
			this.type=type;
		}
	}
		

	
	//------------------------------------------------------------------------------------------	
	//-- AttributeExpr 	
	//------------------------------------------------------------------------------------------
	/**
	 * Abstract class from which all attribute leaf node classes descend.  This
	 * class modifies the behavior of the isAttribute() method.
	 *
	 */
	public abstract class AttributeExpr extends LeafExpr {
		Tuple.Type backingType;

		/**
		 * Initializes common elements of the AttributeExpr hierarchy.  If instantiating
		 * a TagAttributeExpr, using null for the backingType;
		 * @param name The name of the attribute.
		 * @param type The type of the attribute.
		 * @param backingType The type of the attribute in the backing table, or null if
		 * 						it is a tag type.
		 */
		public AttributeExpr(
				String name,
				Type type, 
				Tuple.Type backingType) {
			super(name,type);
			this.backingType = backingType;
			constantFlag=false;

		}
		@Override
		public boolean isAttribute() { return true; }
	
		@Override
		public Result subordinateEval(Tuple t) throws ExprException {
			if (type==Type.Tag) {
				// 1. Get the handle from the tuple
				int handle=0;
				try {
					handle = (Integer)(t.getAttributeValue(handleFieldOffset));
				} catch (Exception e) {
					throw new RuntimeException("Wrong tuple type passed to AttributeExpr.eval()");
				}
				// 2. Ask rm to get a list of all tags with this name for this handle
				TreeSet<String>[] tset = rm.tm_valuesOfTagForHandle(handle,(String)value);
				// 3. Package it in a Result
				return new Result(new TagType(tset),Type.Tag);
			} else {
				Object obj=null;
				try {
					obj = t.getAttributeValue(value.toString());
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
				switch (backingType) {
					case Boolean:
					case Double:
					case Long:
					case String:
						return new Result(obj,type);
					case Integer:
						return new Result(new Long((Integer)obj),type);
					case Instant:
						return  new Result(new TimeStampType((Instant)obj),type);
					default:
				}
			}
			throw new ExprException("Internal error in attribute evaluation");
		}
		
		protected Node xml(Document doc, String derivedClassName) {
			Element nodeEl = doc.createElement(derivedClassName);
			nodeEl.setAttribute("name",(String)value);
			nodeEl.setAttribute("type",type.name());
			nodeEl.setAttribute("backingType",backingType==null?"null":backingType.name());
			return nodeEl;
		}
		
	}

	//------------------------------------------------------------------------------------------	
	//-- BinaryExpr 	
	//------------------------------------------------------------------------------------------
	public abstract class BinaryExpr extends Expr {
		Expr operandA, operandB;
		Operator operator;
		public BinaryExpr(Expr operandA, Operator operator, Expr operandB, Type type) {
			super(type);
			this.operandA=operandA;
			this.operandB=operandB;
			this.operator=operator;
			constantFlag=operandA.constantFlag && operandB.constantFlag;
		}
		

		protected Node xml(Document doc, String derivedClassName) {
			Element nodeEl = doc.createElement(derivedClassName);
			nodeEl.setAttribute("type",type.name());
			nodeEl.setAttribute("operator",operator.name());
			Element op = doc.createElement("operand");
			op.setAttribute("position", "left");
			op.appendChild(operandA.xml(doc));
			nodeEl.appendChild(op);
			op=doc.createElement("operand");
			op.setAttribute("position","right");
			op.appendChild(operandB.xml(doc));
			nodeEl.appendChild(op);
			return nodeEl;
		}
		
		@Override
		public boolean usesTagType() {
			return operandA.usesTagType() || operandB.usesTagType();
		}
		
	}

	//------------------------------------------------------------------------------------------	
	//-- BooleanAttributeExpr 	
	//------------------------------------------------------------------------------------------
	public class BooleanAttributeExpr extends AttributeExpr {
		public BooleanAttributeExpr(String name) {
			super(name,Type.Boolean,Tuple.Type.Boolean);
		}

		@Override
		public Node xml(Document doc) {
			return xml(doc,"BooleanAttributeExpr");
		}
		
}
	
	//------------------------------------------------------------------------------------------	
	//-- BooleanBinaryExpr
	//------------------------------------------------------------------------------------------
	/**
	 * Takes two boolean operands and combines them using EQUAL, NOT EQUAL, AND, and OR.
	 * 
	 */
	public class BooleanBinaryExpr extends BinaryExpr {
		private static final int EQ=0;
		private static final int NE=1;
		private static final int AND=2;
		private static final int OR=3;
		private int op;
		public BooleanBinaryExpr(Expr operandA, Operator operator, Expr operandB) throws ExprException {
			super(operandA,operator,operandB,Type.Boolean);
			if (operandA.type!=Type.Boolean || operandB.type!=Type.Boolean)
				throw new ExprException("Boolean binary operations require boolean operands");
			switch (operator) {
			case Equals:
				op=EQ;
				break;
			case DoesNotEqual:
				op=NE;
				break;
			case BooleanAnd:
				op=AND;
				break;
			case BooleanOr:
				op=OR;
				break;
			default:
				throw new ExprException(String.format("There is no binary operation Boolean %s Boolean",operator.name()));
			}
			prewireConstant();
		}
		
		public Result subordinateEval(Tuple t) throws ExprException {
			Boolean opA = (Boolean)operandA.subordinateEval(t).object;
			Boolean opB = (Boolean)operandB.subordinateEval(t).object;
			boolean res=false;
			switch (op) {
				case EQ: res=opA.equals(opB); break;
				case NE: res= !(opA.equals(opB));
				case AND: res = opA && opB; break;
				case OR: res = opA || opB; break;
			}
			return new Result(value=new Boolean(res),Type.Boolean);
		}

		@Override
		public Node xml(Document doc) {
			return xml(doc,"BooleanBinaryExpr");
		}

		
	}
	
	//------------------------------------------------------------------------------------------	
	//-- BooleanLiteralExpr
	//------------------------------------------------------------------------------------------
	public class BooleanLiteralExpr extends LiteralExpr {
		public BooleanLiteralExpr(boolean value) {
			super(value,Type.Boolean);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"BooleanLiteralExpr");
		}
		

		
	}
	
	//-----------------------------------------------------------------
	// BooleanNotExpr.java
	//-----------------------------------------------------------------

	public class BooleanNotExpr extends UnaryExpr{

		public BooleanNotExpr(Expr operand) throws ExprException {
			super(Operator.BooleanNot,operand,Type.Boolean);
			if (operand.type!=Type.Boolean)
				throw new ExprException(String.format("Unary boolean negation of type %s is not supported",
						operand.type.name()));
			prewireConstant();
		}
		
		@Override
		public Result subordinateEval(Tuple t)  throws ExprException  {
			if (constantFlag)
				return new Result(value,Type.Boolean);
			return new Result(value=!(Boolean)operand.value,Type.Boolean);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"BooleanNotExpr");
		}
		
	}

	//-----------------------------------------------------------------
	// DoubleAttributeExpr.java
	//-----------------------------------------------------------------

	public class DoubleAttributeExpr extends AttributeExpr {
		public DoubleAttributeExpr(String name) {
			super(name,Type.Double,Tuple.Type.Double);
		}
		@Override
		public Node xml(Document doc) {
			return xml(doc,"DoubleAttributeExpr");
		}
	}

	//-----------------------------------------------------------------
	// DoubleBinaryExpr.java
	//-----------------------------------------------------------------

	/**
	 * Takes any combination of Type.Integer and Type.Double operands and combines them
	 * using the specified operation.
	 * 
	 */
	public class DoubleBinaryExpr extends BinaryExpr {
		private static final int EQ=0;
		private static final int NE=1;
		private static final int LT=2;
		private static final int GT=3;
		private static final int LTE=4;
		private static final int GTE=5;
		private int op;
		private int typecase; /* 00=0=double/double, 01=1=double/long, 10=2=long/double, 11=3=long/long */
		
		/**
		 * Creates a new DoubleBinaryExpr. The operands can either be of Type.Integer or
		 * Type.Double.  (Type.Integer values are promoted to Type.Double at Eval time.)
		 * @param operandA The left-hand operand.
		 * @param operator The operator.
		 * @param operandB The right-hand operand.
		 * @throws ExprException
		 */
		public DoubleBinaryExpr(Expr operandA, Operator operator, Expr operandB) throws ExprException {
			super(operandA,operator,operandB,Type.Boolean);
			
			switch (operandA.type) {
				case Double: typecase=0; break;
				case Integer: typecase=2; break;
				default: typecase=-1;
			}
			if (operandB.type==Type.Integer)
				typecase = typecase + 1;
			if (typecase<0)
				throw new ExprException(String.format("Incompatible types for Double binary operation: %s and %s",
						operandA.type.name(),operandB.type.name()));
		
			switch (operator) {
			case Equals:
				op=EQ;
				break;
			case DoesNotEqual:
				op=NE;
				break;
			case LessThan:
				op=LT;
				break;
			case GreaterThan:
				op=GT;
				break;
			case LessThanOrEqualTo:
				op=LTE;
				break;
			case GreaterThanOrEqualTo:
				op=GTE;
				break;
			default:
				throw new ExprException(String.format("There is no binary operation Double %s Double",operator.name()));
			}
			prewireConstant();
		}
		
		/**
		 * Computes the result of combining the two Type.Double operands.  Type.Integer operands
		 * are promoted to Type.Double operands before performing the operation.
		 * @param t The tuple context.  
		 */
		public Result subordinateEval(Tuple t)  throws ExprException {
			if (constantFlag)
				return new Result(value,Type.Boolean);
			Result rA = operandA.subordinateEval(t);
			Result rB = operandB.subordinateEval(t);
			Double A, B;
			
			A = ((typecase&2)==0) ? (Double)rA.object : (Long)rA.object;
			B = ((typecase&1)==0) ? (Double)rB.object : (Long)rB.object;
			
			boolean res = false;
			int comp = A.compareTo(B);
			switch (op) {
				case EQ: res = comp==0; break;
				case NE: res = comp!=0; break;
				case LT: res = comp<0; break;
				case GT: res = comp>0; break;
				case LTE: res=comp<=0; break;
				case GTE: res=comp>=0; break;
			}
			return new Result(value=res,Type.Boolean);
		}
	
		@Override
		public Node xml(Document doc) {
			return xml(doc,"DoubleBinaryExpr");
		}


		
	}

	//-----------------------------------------------------------------
	// DoubleLiteralExpr.java
	//-----------------------------------------------------------------

	public class DoubleLiteralExpr extends LiteralExpr {
		public DoubleLiteralExpr(double value) {
			super(value,Type.Double);
		}
		@Override
		public Node xml(Document doc) {
			return xml(doc,"DoubleLiteralExpr");
		}

	}


	//-----------------------------------------------------------------
	// IntegerAttributeExpr.java
	//-----------------------------------------------------------------

	public class IntegerAttributeExpr extends AttributeExpr {
		
		
		public IntegerAttributeExpr(String name, boolean is32bit) {
			super(name,Type.Integer,is32bit?Tuple.Type.Integer:Tuple.Type.Long);
		}
		
		@Override
		public Result subordinateEval(Tuple t)  throws ExprException {
			Object storedValue = null;
			try {
				storedValue = t.getAttributeValue((String)value);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			if (backingType==Tuple.Type.Integer)
				return new Result( new Long((Integer)storedValue), Type.Integer);
			return new Result(storedValue,Type.Integer);
		}

		@Override
		public Node xml(Document doc) {
			return xml(doc,"IntegerAttributeExpr");
		}
	}

	//-----------------------------------------------------------------
	// IntegerBinaryExpr.java
	//-----------------------------------------------------------------

	/**
	 * Takes any two operands of Type.Integer operands and combines them
	 * using the specified operation.
	 *
	 */
	public class IntegerBinaryExpr extends BinaryExpr {
		private static final int EQ=0;
		private static final int NE=1;
		private static final int LT=2;
		private static final int GT=3;
		private static final int LTE=4;
		private static final int GTE=5;
		private int op;
		public IntegerBinaryExpr(Expr operandA, Operator operator, Expr operandB) throws ExprException {
			super(operandA,operator,operandB,Type.Boolean);
			if (operandA.type!=Type.Integer || operandB.type!=Type.Integer)
				throw new ExprException("Integer binary operations require Integer operands");
			switch (operator) {
			case Equals:
				op=EQ;
				break;
			case DoesNotEqual:
				op=NE;
				break;
			case LessThan:
				op=LT;
				break;
			case GreaterThan:
				op=GT;
				break;
			case LessThanOrEqualTo:
				op=LTE;
				break;
			case GreaterThanOrEqualTo:
				op=GTE;
				break;
			default:
				throw new ExprException(String.format("There is no binary operation Integer %s Integer",operator.name()));
			}
			prewireConstant();
		}
		
		public Result subordinateEval(Tuple t) throws ExprException  {
			if (constantFlag)
				return new Result(value,Type.Boolean);
			Long opA = (Long)operandA.subordinateEval(t).object;
			Long opB = (Long)operandB.subordinateEval(t).object;
			boolean res = false;
			int comp = opA.compareTo(opB);
			switch (op) {
				case EQ: res = comp==0; break;
				case NE: res = comp!=0; break;
				case LT: res = comp<0; break;
				case GT: res = comp>0; break;
				case LTE: res=comp<=0; break;
				case GTE: res=comp>=0; break;
			}
			return new Result(value=res,Type.Boolean);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"IntegerBinaryExpr");
		}


	}

	//-----------------------------------------------------------------
	// IntegerLiteralExpr.java
	//-----------------------------------------------------------------

	public class IntegerLiteralExpr extends LiteralExpr {
		public IntegerLiteralExpr(long value) {
			super(value,Type.Integer);
		}

		@Override
		public Node xml(Document doc) {
			return xml(doc,"IntegerLiteralExpr");
		}

	}

	//-----------------------------------------------------------------
	// LeafExpr.java
	//-----------------------------------------------------------------

	/**
	 * Abstract class from which all leaf types descend.  All leaves have a
	 * value attribute which is either a literal value, or an attribute/tag
	 * name.  This class also overrides the behavior of the isLeaf() method.
	 */
	public abstract class LeafExpr extends Expr {
		
		/** Create with a specific value and type **/
		public LeafExpr(Object value, Type type) {
			super(type);
			this.value = value;
		}
		
		@Override
		public boolean isLeaf() { return true; }


	}

	//-----------------------------------------------------------------
	// LiteralExpr.java
	//-----------------------------------------------------------------

	/**
	 * Abstract class from which all literal leaf node classes descend.  This class
	 * has the behavior that evaluation returns the actual value of the value attribute.
	 * Each descendent implements a typed method accordingly.  This class modifies the 
	 * behavior of the isLiteral() method and provides a concrete eval() method that
	 * creates a result using the value and type attributes of the object.
	 *
	 */
	public abstract class LiteralExpr extends LeafExpr {
		
		/** Create with a specific value and a type **/
		public LiteralExpr(Object value, Type type) {
			super(value,type);
			constantFlag=true;
		}
		
		@Override
		public boolean isLiteral() { return true; }

		@Override
		public Result subordinateEval(Tuple t) {
			return new Result(value,type);
		}
		
		public Node xml(Document doc, String derivedTypeName) {
			Element nodeEl = doc.createElement(derivedTypeName);
			nodeEl.setAttribute("type", type.name());
			nodeEl.setAttribute("value", value.toString());
			return nodeEl;
		}
		
	}

	//-----------------------------------------------------------------
	// StringAttributeExpr.java
	//-----------------------------------------------------------------

	public class StringAttributeExpr extends AttributeExpr {
		public StringAttributeExpr(String name) {
			super(name,Type.String,Tuple.Type.String);
		}

		@Override
		public Node xml(Document doc) {
			return xml(doc,"StringAttributeExpr");
		}
	}

	//-----------------------------------------------------------------
	// StringBinaryExpr.java
	//-----------------------------------------------------------------

	public class StringBinaryExpr extends BinaryExpr  {
		private static final int EQ=0;
		private static final int NE=1;
		private static final int LT=2;
		private static final int GT=3;
		private static final int LTE=4;
		private static final int GTE=5;
		private static final int SUB=6;
		private int op;
		private boolean ci=false;
		public StringBinaryExpr(Expr operandA, Operator operator, Expr operandB) throws ExprException {
			super(operandA,operator,operandB,Type.Boolean);
			if (operandA.type!=Type.String || operandB.type!=Type.String)
				throw new ExprException("String binary operations require string operands");
			switch (operator) {
			case CI_Equals:
				ci=true;
			case Equals:
				op=EQ;
				break;
			case CI_DoesNotEqual:
				ci=true;
			case DoesNotEqual:
				op=NE;
				break;
			case CI_LessThan:
				ci=true;
			case LessThan:
				op=LT;
				break;
			case CI_LessThanOrEqualTo:
				ci=true;
			case LessThanOrEqualTo:
				op=LTE;
				break;
			case CI_GreaterThan:
				ci=true;
			case GreaterThan:
				op=GT;
				break;
			case CI_GreaterThanOrEqualTo:
				ci=true;
			case GreaterThanOrEqualTo:
				op=GTE;
				break;
			case CI_IsASubstringOf:
				ci=true;
			case IsASubstringOf:
				op=SUB;
				break;
			default:
				throw new ExprException(String.format("There is no binary operation String %s String",operator.name()));
			}
			prewireConstant();
		}
		
		public Result subordinateEval(Tuple t)  throws ExprException {
			if (constantFlag)
				return new Result(value,Type.Boolean);
			String opA = (String)operandA.subordinateEval(t).object;
			String opB = (String)operandB.subordinateEval(t).object;
			boolean res = false;
			if (op==SUB) {
				if (ci) {
					opA=opA.toLowerCase();
					opB=opB.toLowerCase();
				}
				res = opB.contains(opA);
			} else {
				int comp = ci ? opA.compareToIgnoreCase(opB) : opA.compareTo(opB);
				switch (op) {
					case EQ: res = comp==0; break;
					case NE: res = comp!=0; break;
					case LT: res = comp<0; break;
					case GT: res = comp>0; break;
					case LTE: res = comp<=0; break;
					case GTE: res = comp>=0; break;
					case SUB: break; /* can't happen */
				}
			}
			return new Result(value=res,Type.Boolean);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"StringBinaryExpr");
		}


	}

	//-----------------------------------------------------------------
	// StringLiteralExpr.java
	//-----------------------------------------------------------------

	public class StringLiteralExpr extends LiteralExpr {
		public StringLiteralExpr(String value) {
			super(value,Type.String);
		}
		@Override
		public Node xml(Document doc) {
			return xml(doc,"StringLiteralExpr");
		}

	}

	//-----------------------------------------------------------------
	// TagAttributeExpr.java
	//-----------------------------------------------------------------

	public class TagAttributeExpr extends AttributeExpr {
		public TagAttributeExpr(String name) {
			super(name,Type.Tag,null);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"TagAttributeExpr");
		}
		
		@Override 
		public boolean usesTagType() {
			return true;
		}

	}

	//-----------------------------------------------------------------
	// TagBinaryExpr.java
	//-----------------------------------------------------------------

	/**
	 * Takes either two tag type expressions, or a tag type expression on the left
	 * and a string on the right.  In the case of the first, the node will perform
	 * set comparison operations between the two tag type values.  For the second case,
	 * the node will perform set membership tests.
	 *
	 */
	public class TagBinaryExpr extends BinaryExpr {
		private static final int EQ=0;
		private static final int NE=1;
		private static final int INC=2;
		private static final int NINC=3;
		private int op;
		private boolean ci=false;
		private boolean membershipTest = false;
		
		public TagBinaryExpr(Expr operandA, Operator operator, Expr operandB) throws ExprException {
			super(operandA,operator,operandB,Type.Boolean);
			membershipTest = operandB.type==Type.String;
			if (operandA.type!=Type.Tag && !(operandB.type==Type.Tag || membershipTest))
				throw new ExprException(String.format("Binary operations with types %s and %s are not supported",
						operandA.type.name(),operandB.type.name()));
			switch (operator) {
			case CI_Equals:
				ci=true;
			case Equals:
				op = membershipTest?INC:EQ;
				break;
			case CI_DoesNotEqual:
				ci=true;
			case DoesNotEqual:
				op = membershipTest?NINC:NE;
			break;
			default:
				throw new ExprException(String.format("Binary operation %s %s %s not supported",
						operandA.type.name(),operator.name(),operandB.type.name()));
			}
			prewireConstant();
		}
		
		public Result subordinateEval(Tuple t)  throws ExprException {
			if (constantFlag)
				return new Result(value,Type.Boolean);
			TagType opA = (TagType)operandA.subordinateEval(t).object;
			boolean res;
			if (membershipTest) {
				String opB = (String)operandB.subordinateEval(t).object;
				res= opA.contains(opB,ci);
				if (op==NINC)
					res=!res;
			} else /* equality test */ {
				TagType opB = (TagType)operandB.subordinateEval(t).object;
				res= opA.equals(opB,ci);
				if (op==NE)
					res = !res;
			}
			return new Result(value=res,Type.Boolean);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"TagBinaryExpr");
		}
		
		@Override 
		public boolean usesTagType() {
			return true;
		}



	}

	//-----------------------------------------------------------------
	// TagLiteralExpr.java
	//-----------------------------------------------------------------

	public class TagLiteralExpr extends LiteralExpr {
		public TagLiteralExpr(TagType value) {
			super(value,Type.Tag);
		}
		@Override
		public Node xml(Document doc) {
			return xml(doc,"TagLiteralExpr");
		}
		
		@Override 
		public boolean usesTagType() {
			return true;
		}


	}

	//-----------------------------------------------------------------
	// TimeStampAttributeExpr.java
	//-----------------------------------------------------------------


	public class TimeStampAttributeExpr extends AttributeExpr {
		public TimeStampAttributeExpr(String name) {
			super(name,Type.TimeStamp,Tuple.Type.Instant);
		}

		@Override
		public Node xml(Document doc) {
			return xml(doc,"BooleanAttributeExpr");
		}
	}

	//-----------------------------------------------------------------
	// TimeStampBinaryExpr.java
	//-----------------------------------------------------------------

	/**
	 * Takes any combination of Type.TimeStamp and Type.String operands where there is
	 * at most one Type.String operand and combines them
	 * using the specified operation.
	 *
	 */
	public class TimeStampBinaryExpr extends BinaryExpr {

		/**
		 * Creates a binary operation for TimeStamp comparison. Operands may be any TimeStamp type,
		 * or StringLiteralExpr nodes.
		 *    
		 * @param operandA
		 * @param operator
		 * @param operandB
		 * @throws ExprException
		 */
		public TimeStampBinaryExpr(Expr operandA, Operator operator, Expr operandB) throws ExprException {
			super(operandA,operator,operandB,Type.Boolean);
			if (operandA.type==Type.String && operandA.constantFlag)
				this.operandA = operandA = new TimeStampLiteralExpr((String)operandA.value);
			if (operandB.type==Type.String && operandB.constantFlag)
				this.operandB = operandB = new TimeStampLiteralExpr((String)operandB.value);
			
			switch (operator) {
			case Equals:
			case DoesNotEqual:
			case LessThan:
			case GreaterThan:
			case LessThanOrEqualTo:
			case GreaterThanOrEqualTo:
				break;
			default:
				throw new ExprException(String.format("There is no binary operation TimeStamp %s TimeStamp",operator.name()));
			}
			prewireConstant();
		}
		
		public Result subordinateEval(Tuple t)  throws ExprException {
			if (constantFlag)
				return new Result(value,Type.Boolean);
			boolean res = false;
			TimeStampType opA = (TimeStampType)operandA.subordinateEval(t).object;
			TimeStampType opB = (TimeStampType)operandB.subordinateEval(t).object;
			int comp = opA.compareTo(opB);
			switch (operator) {
				case Equals: res = comp==0; break;
				case DoesNotEqual: res = comp!=0; break;
				case LessThan: res = comp<0; break;
				case GreaterThan: res = comp>0; break;
				case LessThanOrEqualTo: res=comp<=0; break;
				case GreaterThanOrEqualTo: res=comp>=0; break;
				default: /* shouldn't happen */
			}
			return new Result(value=res,Type.Boolean);
		}
		
		@Override
		public Node xml(Document doc) {
			return xml(doc,"TimeStampBinaryExpr");
		}


	}

	//-----------------------------------------------------------------
	// TimeStampLiteralExpr.java
	//-----------------------------------------------------------------


	public class TimeStampLiteralExpr extends LiteralExpr {
		public TimeStampLiteralExpr(TimeStampType value) {
			super(value,Type.TimeStamp);
		}
		public TimeStampLiteralExpr(Instant value) {
			super(new TimeStampType(value),Type.TimeStamp);
		}
		public TimeStampLiteralExpr(String value) {
			super(new TimeStampType(value),Type.TimeStamp);
		}
		public TimeStampLiteralExpr() {
			super(new TimeStampType(),Type.TimeStamp);
		}
		@Override
		public Node xml(Document doc) {
			return xml(doc,"TimeStampLiteralExpr");
		}

	}

	//-----------------------------------------------------------------
	// UnaryExpr.java
	//-----------------------------------------------------------------


	/**
	 * Class from which unary operations nodes descend.
	 *
	 */
	public abstract class UnaryExpr extends Expr {
		
		public Operator operator;
		public Expr operand;
		
		public UnaryExpr(Operator operator, Expr operand, Type type) {
			super(type);
			this.operator = operator;
			this.operand = operand;
			constantFlag = operand.constantFlag;
		}
		
		public Node xml(Document doc, String derivedTypeName) {
			Element nodeEl = doc.createElement(derivedTypeName);
			nodeEl.setAttribute("type",type.name());
			nodeEl.setAttribute("operator",operator.name());
			Element op = doc.createElement("operand");
			op.appendChild(operand.xml(doc));
			nodeEl.appendChild(op);
			return nodeEl;
		}
		
	}


	
}
