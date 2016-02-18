package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.time.Instant;

import com.craiglowery.java.vlib.tuple.Tuple.Type;

/** Expands a Tuple expression tree into Postgresl SQL equiavlent. **/

public class PostgresqlExpressionGenerator {
	

	private static String par(String s) {
		return new StringBuilder("(").append(s).append(")").toString();
	}
	
	public static String expand(TupleExpressionFactory.Node node)
		throws FilterExpressionException
	{
		
		if (node.type==Type.String && node.kind==Kind.LITERAL) 
			// Literal string must be quoted and escaped for SQL
			return par(node.escapedAndQuoted("'", "''", "'"));
		if (node.kind==Kind.ATTRIBUTE || node.kind==Kind.LITERAL) 
			//For Postgresql, we will simply retrieve the string and put parentheses around it.
			return par(node.toString());
		if (node.kind==Kind.AND | node.kind==Kind.OR) 
			//AND and OR multi operand nodes expand the same way, just the operator name is different
			return par(multiArgExpand(node.asMultiArg()));
		if (node.kind==Kind.NOT) 
			return par("NOT("+expand(node.asNot().target)+")");
		if (node.kind==Kind.COMPARISON) 
			return par(expandComparisonNode(node.asComparison()));
		throw new FilterExpressionException(String.format("Node subclass '%s' not supported by SQLExpressionGenerator",node.getClass().getName()));
	}
	
	private static String multiArgExpand(TupleExpressionFactory.MultiArgumentNode multiNode )
		throws FilterExpressionException
	{
		// The structure is   (leftexpanded)op(rightexpanded) for the first part (at least 2) args
		String op=   (multiNode.kind==Kind.AND?"AND":"OR");
		
		StringBuilder sb = new StringBuilder
				   (expand(multiNode.targets.get(0)))
			.append(op)
			.append(expand(multiNode.targets.get(1)));
		
		// We then append addition args as  op(additional)
		
		for (int x=2; x<multiNode.targets.size(); x++) {
			sb.append(op)
			  .append(expand(multiNode.targets.get(x)));
		}
		return sb.toString();
	}
	
	private static String[] opStrings = {
		"=",   //EQUAL_TO,                     		// ==   =
		"<>",  //NOT_EQUAL_TO,            			// !=   <>
		"<",   //LESS_THAN,							// <
		"<=",  //LESS_THAN_OR_EQUAL_TO,				// <=
		">",   //GREATER_THAN,						// >
		">=",  //GREATER_THAN_OR_EQUAL_TO,			// >=
		null,  //IS_SUBSTRING_OF,							// $
		"=",   //CI_EQUAL_TO,                     	// ~==   ~=
		"<>",  //CI_NOT_EQUAL_TO,            		// ~!=   ~<>
		"<",   //CI_LESS_THAN,						// ~<
		"<=",  //CI_LESS_THAN_OR_EQUAL_TO,			// ~<=
		">",   //CI_GREATER_THAN,					// ~>
		">=",  //CI_GREATER_THAN_OR_EQUAL_TO,		// ~>=
		null   //CI_IS_SUBSTRING_OF,						// ~$
	};
	
	private static String expandComparisonNode(TupleExpressionFactory.ComparisonNode compNode)
		throws FilterExpressionException
	{
		// With a few corner cases, the expansion is (left)op(right)
		
		//For most operations, the below are the appropriate operands. Just need an operator between them
		Boolean thisIsCiOp = compNode.op.ordinal()>=Operator.CI_EQUAL_TO.ordinal();
		String left  = (thisIsCiOp?"lower":"") + expand(compNode.left);
		String right = (thisIsCiOp?"lower":"") + expand(compNode.right);

		switch (compNode.op) {
			case EQUAL_TO:	
			case NOT_EQUAL_TO:	
			case LESS_THAN:	
			case GREATER_THAN:	
			case LESS_THAN_OR_EQUAL_TO:	
			case GREATER_THAN_OR_EQUAL_TO:
				if (compNode.left.type==Type.Instant || compNode.left.type==Type.Instant) {
					if (compNode.right.type==Type.Instant ^ compNode.left.type==Type.Instant)
						throw new FilterExpressionException(String.format("INSTANT comparison operands have type mismatch: %s(%s)%s%s(%s)",
								left,compNode.left.type.name(),opStrings[compNode.op.ordinal()],right,compNode.right.type.name()));
					// At this point, we know we have INSTANT's on both sides.
					if (compNode.left.kind==Kind.LITERAL) {
						Instant i = SmartDateTimeParser.tryParse(compNode.left.toString());
						if (i==null)
							throw new FilterExpressionException(String.format("Datetime parse error for %s",left));
						left = par(i.toString());
					}
					if (compNode.right.kind==Kind.LITERAL) {
						Instant i = SmartDateTimeParser.tryParse(compNode.right.toString());
						if (i==null)
							throw new FilterExpressionException(String.format("Datetime parse error for %s",right));
						right = par("'"+i.toString()+"'");
					}
					return new StringBuilder(left).append(opStrings[compNode.op.ordinal()]).append(right).toString();
				}
			case CI_EQUAL_TO:	
			case CI_NOT_EQUAL_TO:	
			case CI_LESS_THAN:	
			case CI_GREATER_THAN:	
			case CI_LESS_THAN_OR_EQUAL_TO:	
			case CI_GREATER_THAN_OR_EQUAL_TO:
				return new StringBuilder(left).append(opStrings[compNode.op.ordinal()]).append(right).toString();
			case IS_SUBSTRING_OF:
			case CI_IS_SUBSTRING_OF:
				//The semantic meaning passed to us is needle IS_SUBSTRING_OF haystack
				//In Postgresql, we can use the strpos(haystack,needle) function. If it
				//returns non zero, it is a match.
				return new StringBuilder("strpos(").append(right).append(",").append(left).append(")>0").toString();
			default:
				throw new FilterExpressionException(String.format("Unknown comparison operator ordinal value %d",compNode.op.ordinal()));
		}
	}

}
