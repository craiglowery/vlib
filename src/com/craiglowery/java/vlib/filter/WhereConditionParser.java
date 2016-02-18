package com.craiglowery.java.vlib.filter;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.io.StringReader;
import com.craiglowery.java.vlib.filter.ExpressionFactory.Expr;
import com.craiglowery.java.vlib.filter.ExpressionFactory;

import java_cup.runtime.Symbol;

/**
 * Methods to parse text expressions for use in querying the database safely.
 *
 */
public class WhereConditionParser {

	/**
	 * Parses a string and returns an ExpressionFactory.Expr rooting the expression tree parsed, 
	 * or null if the string doesn not parse, or is not considered a safely transformable query.
	 * 
	 * Details of the syntax are as follows:<p>
	 * <ul>
	 * 
	 * 	  <li>Tokens are not case sensitive.
	 * 	  <li>An <i>expression</i> is one of the following:
	 *        <dl>
	 *           <dt> A literal value.
	 *           <dd> These are constant values drawn from the six supported types:
	 *               <dl>
	 *             
	 *                  <dt> String Literal
	 *                  <dd> Strings can be delimited with single quotes are double quotes.
	 *            			 An instance of the delimited quote character can be escaped in the string
	 *             			 by a repeating sequence of two of them.
	 *             
	 *             		<dt> TimeStamp Literal
	 *             		<dd> These are String Literals which, when found in operational contexts
	 *             			 adjacent to a known TimeStamp expression, such as an attribute of that
	 *     					 type, are parsed to extract a TimeStamp value.  The parser is very
	 *     				     generous it its interpretation, so most reasonable formats can be parsed. 

	 *             		<dt> Integer Literal
	 *             		<dd> A sequence of characters that specify a decimal, octal, or hexadecimal
	 *                       encoded integer value.  The actual underlying datatype is
	 *             			Java {@code Long}.  Octal values start with a prefix of '0' and
	 *             			hexadecimal values start with '0x' or '0X'.  An optional negative sign
	 *             			 can precede the expression to indicate a negative value.
	 *             
	 *             		<dt> Double Literal
	 *             		<dd> A floating point number, which is a sequence of decimal digits with 
	 *             			 exactly one embedded decimal point (.).
	 *             
	 *             		<dt> Boolean Literal
	 *             		<dd> The tokens TRUE or FALSE.
	 *             
	 *             		<dt> Tag Literal
	 *             		<dd> A tag is a set of strings.  Tag literals are comma separated lists of
	 *             			 String literals within a pair of brackets. For example, the tag value for
	 *             			 Primary Colors could be denoted:<p>
	 *             			 <ul>
	 *             					{@code ["red","yellow","blue"]}
	 *             			 <ul>
	 *               </dl>
	 *            <dt> An attribute name.
	 *            <dd> A sequence of characters that constitute a legal Java identifier, which references
	 *            	   either an attribute in the tuple {@code T} or is the name of a tag in the
	 *                 greater tagging system.  Attribute names are case sensitive, as in Java.
	 *                 Tag names that include spaces are not accessible through this interface.
	 *            <dt> A binary operation.
	 *            <dd> All binary operations result in a Boolean typed value.  The operators are:
	 *            	<table style="border: 3px solid;">
	 *            	  <col style="border: 1px solid black;">
	 *            	  <col style="border: 1px solid black;">
	 *            	  <col style="border: 1px solid black;">
	 *            	  <col style="border: 1px solid black;">
	 *            	  <col style="border: 1px solid black;">
	 *            	  <col style="border: 1px solid black;">
	 *                <tr><th>Operator Name</th><th>Syntax and Variations</th><th>Left Operand A</th><th>Right Operand B</th><th>Notes</th></tr>
	 *                <tr style="border: 1px black solid;">
	 *                  <td>Equals</td>
	 *                  <td>{@code ==}<br>{@code =}</td>
	 *                  <td><i>any type</i></td>
	 *                  <td><i>same type</i></td>
	 *                  <td>True if A and B are the same value.  For Tag types, this means they have the same set membership.
	 *                </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Not Equals</td>
	 *                	<td>{@code !=}<br>{@code <>}</td>
	 *                	<td><i>any type</i></td>
	 *                	<td><i>same type</i></td>
	 *                	<td>True if A and B are different values. For Tag types, this means they differ in set membership.</td>
	 *                </tr style="border: 1px black solid;">
	 *                <tr>
	 *                	<td>Less Than</td>
	 *                	<td>{@code <}</td>
	 *                	<td><i>any type except Tag</i></td>
	 *                	<td><i>same type</i></td>
	 *                	<td>True if A precedes B in a natural ordering.</td>
	 *                </tr style="border: 1px black solid;">
	 *                <tr>
	 *                	<td>Less Than Or Equal To</td>
	 *                	<td>{@code <=}</td>
	 *                	<td><i>any type except Tag</i></td>
	 *                	<td><i>same type</i></td>
	 *                	<td>True if A precedes B in a natural ordering, or is equal to B.</td>
	 *                </tr style="border: 1px black solid;">
	 *                <tr>
	 *                	<td>Greater Than</td>
	 *                	<td>{@code >}</td>
	 *                	<td><i>any type except Tag</i></td>
	 *                	<td><i>same type</i></td>
	 *                	<td>True if A follows B in a natural ordering.</td>
	 *                </tr style="border: 1px black solid;">
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Greater Than Or Equal To</td>
	 *                	<td>{@code >=}</td>
	 *                	<td><i>any type except Tag</i></td>
	 *                	<td><i>same type</i></td>
	 *                	<td>True if A follows B in a natural ordering, or is equal to B.</td>
	 *                </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Is A Substring Of</td>
	 *                	<td>{@code $}</td>
	 *                	<td>{@code String}</td>
	 *                	<td>{@code String}</td>
	 *                	<td>True if A is a substring of B.</td>
	 *                </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Case Insensitive String Operation</td>
	 *                	<td>{@code ~==  ~=}<br>
	 *                      {@code ~!= ~<>}<br>
	 *                      {@code ~<}<br>
	 *                      {@code ~>}<br>
	 *                      {@code ~<=}<br>
	 *                      {@code ~>=}<br>
	 *                      {@code ~$}</td>
	 *                	<td>{@code String}</td>
	 *                	<td>{@code String}</td>
	 *                	<td>Each comparison that can be performed with string can be made case
	 *                		insensitive by prepending the operator with a tilde (~).</td>
	 *				  </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Includes</td>
	 *                	<td>{@code =}<br>{@code ==}</td>
	 *                	<td>{@code Tag}</td>
	 *                	<td>{@code String}</td>
	 *                	<td>True if string B is in the Tag set A.</td>
	 *                </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Does Not Include</td>
	 *                	<td>{@code !=}<br>{@code !<>}</td>
	 *                	<td>{@code Tag}</td>
	 *                	<td>{@code String}</td>
	 *                	<td>True if string B is not in the Tag set A.</td>
	 *                </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Logical Conjunction</td>
	 *                	<td>{@code AND}</td>
	 *                	<td>{@code Boolean}</td>
	 *                	<td>{@code Boolean}</td>
	 *                	<td>True if both A and B are true.</td>
	 *                </tr>
	 *                <tr style="border: 1px black solid;">
	 *                	<td>Logical Disjunction</td>
	 *                	<td>{@code OR}</td>
	 *                	<td>{@code Boolean}</td>
	 *                	<td>{@code Boolean}</td>
	 *                	<td>True if either A or B or both A and B are true.</td>
	 *                </tr>
	 *              </table>
	 *            
	 *            <dt> A unary operation.
	 *            <dd> There is only one unary operation, Logical Negation, represented by 
	 *            	the token {@code NOT}.  It is followed by a single Boolean operand, and
	 *            	returns the opposite of the operand.
	 *            <dt> A forced precedence.
	 *            <dd> Any expression can be set inside parentheses to guarantee a specific order
	 *                of evaluation within a larger expression string.
	 *        </dl>
	 * 
	 * </ul>       
	 *          
	 * @param condition The string containing the expression to be parsed.
	 * @param factory The {@code ExpressionFactory<T>} that provides context for attribute name
	 * and type resolution from Tuple type T, as well as the source of expression nodes.
	 * @return The Expr object rooting the expression tree resulting from a successful parse.  The 
	 *          method throws an ExprException if a failure occurs.
	 */
	@SuppressWarnings("rawtypes")
	public static Expr parseFilterExpression(String condition, ExpressionFactory factory) 
		throws Exception
	{ 
		Yylex lexer = new Yylex(new StringReader(condition));
		parser p = new parser(lexer, factory);
		try {
			Symbol result = p.parse();
			
			Expr top =(Expr)(result.value);
			return top;
		} catch (ExprParsingException epe) {
			epe.expression=condition;
			throw epe;
		} catch (Exception e) {
			Throwable cause = e.getCause();
			if (cause!=null && cause.getClass()==ExprParsingException.class) {
				ExprParsingException epe = (ExprParsingException)cause;
				epe.expression = condition;
				throw epe;
			}			
			if (cause!=null && cause.getClass().isAssignableFrom(ExprException.class))
				throw (ExprException)cause;
			throw e;
		}
	}


}
