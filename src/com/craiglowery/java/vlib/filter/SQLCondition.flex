package com.craiglowery.java.vlib.filter;
import java_cup.runtime.*;


%%

%cup
%unicode
%ignorecase
%column

%yylexthrow{
	 ExprException
%yylexthrow}

%{
	StringBuffer string = new StringBuffer();
	
	private Symbol symbol(int type) {
		return new Symbol(type, yycolumn, yycolumn+yytext().length()-1);
	}

	private Symbol symbol(int type, Object value) {
		return new Symbol(type, yycolumn, yycolumn+yytext().length()-1, value);
	}

%}

Identifier = [a-zA-Z][a-zA-Z0-9_]*
IntegerLiteral = -?([1-90-9]+ | 0[0-7]* | 0x[0-9A-B]+)
DoubleLiteral =  -?([0-9]*\.[0-9]+ | [0-9]+\.[0-9]*)
SingleQuotedString = '(('')|[^'])*'
DoubleQuotedString = \"((\"\")|[^\"])*\"
TimeStampLiteral = \{[0-9A-Za-z :,\-/\.]*\}


%%

<YYINITIAL>	{

  "<>" | "!=" 		{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.DoesNotEqual); } 
  "~<>" | "~!=" 	{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_DoesNotEqual); } 

  "==" | "="		{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.Equals); }
  "~==" | "~="		{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_Equals); }

  "<="				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.LessThanOrEqualTo); }
  "~<="				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_LessThanOrEqualTo); }
  
  ">="				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.GreaterThanOrEqualTo); }
  "~>="				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_GreaterThanOrEqualTo); }
  
  "<"				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.LessThan); }
  "~<"				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_LessThan); }
  
  ">"				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.GreaterThan); }
  "~>"				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_GreaterThan); }

  "$"				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.IsASubstringOf); }
  "~$"				{ return symbol(sym.COMPARISON_OPERATOR,ExpressionFactory.Operator.CI_IsASubstringOf); }
  
  
  AND 				{ return symbol(sym.BOOLEAN_AND_OPERATOR,ExpressionFactory.Operator.BooleanAnd); }
  OR 				{ return symbol(sym.BOOLEAN_OR_OPERATOR,ExpressionFactory.Operator.BooleanOr); }

  TRUE				{ return symbol(sym.TRUE_TOKEN,yytext()); }
  FALSE				{ return symbol(sym.FALSE_TOKEN,yytext()); }
  
  NOT | "!"			{ return symbol(sym.BOOLEAN_NOT_OPERATOR,ExpressionFactory.Operator.BooleanNot); }
  
  "("				{ return symbol(sym.OPEN_PAREN_TOKEN); }
  "["				{ return symbol(sym.OPEN_BRACKET_TOKEN); }
  ")"				{ return symbol(sym.CLOSE_PAREN_TOKEN); }
  "]"				{ return symbol(sym.CLOSE_BRACKET_TOKEN); }
  
  {Identifier}		{ 
  						return symbol(sym.ATTRIBUTE_NAME_STRING, yytext()); 
  					}

  {SingleQuotedString}			{ 
									  String v = yytext();
									  v = v.substring(1,v.length()-1).replace("''","'");
									  return symbol(sym.GENERAL_VALUE_STRING, v);
								}

  {DoubleQuotedString}			{ 
									  String v = yytext();
									  v = v.substring(1,v.length()-1).replace("\"\"","\"");
									  return symbol(sym.GENERAL_VALUE_STRING, v);
								}

  {IntegerLiteral} 				{ long i;
 									try {
 										i = Long.decode(yytext());
 										System.out.println("It worked");
 									} catch (Exception e) {
 										throw new ExprParsingException("Integer literal could not be parsed",null,yycolumn);
 									}						
 									return symbol(sym.GENERAL_VALUE_INTEGER, new Long(i));
            }

 								
  {DoubleLiteral}				{
  									try {
   										return symbol(sym.GENERAL_VALUE_DOUBLE,new Double(yytext()));
   									} catch (Exception e) {
   										throw new ExprParsingException("Double literal could not be parsed",null,yycolumn);
   									}
  								}
  								
  {TimeStampLiteral}			{
  									try {
									  String v = yytext();
									  v = v.substring(1,v.length()-1).replace("''","'");
									  return symbol(sym.TIMESTAMP_VALUE_STRING, v);
  									} catch (Exception e) {
  										throw new ExprParsingException("TimeStamp literal could not be parsed",null,yycolumn);
  									}
  								}
 
									
 ","							{ return symbol(sym.COMMA_TOKEN,yytext()); }
  
  " "							{ /* ignore */ }
  
 [^]							{
  									throw new ExprParsingException("Unmatched input",null,yycolumn);
  								}
  
}
