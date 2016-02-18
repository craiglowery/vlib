package com.craiglowery.java.vlib.tuple.filterexp;
import java_cup.runtime.*;


%%

%class StringExpressionScanner
%cupsym StringExpressionSymbols
%cup
%unicode
%ignorecase
%column



%{
	StringBuffer string = new StringBuffer();
	
	private Symbol symbol(int type) {
		return new Symbol(type, yyline, yycolumn);
	}

	private Symbol symbol(int type, Object value) {
		return new Symbol(type, yyline, yycolumn, value);
	}

%}

Identifier = [a-zA-Z][a-zA-Z0-9_]*

DecIntegerLiteral =  0 | [1-9][0-9]*
Double =   [0-9]*\.[0-9]+ | [0-9]+\.[0-9]*
String1 = '(('')|[^'])*'
String2 = \"[^\"]*\" 



%%

<YYINITIAL>	{

  "==" | "=" | "!=" | "<>" | "<" | "<=" | ">" | ">=" | "$" |"~==" | "~=" | "~!=" | "~<>" | "~<" | "~<=" | "~>" | "~>=" | "~$"
  	 		{ return symbol(StringExpressionSymbols.COMPARISON_OPERATOR,yytext()); }
  
  [aA][nN][Dd] | &&				{ return symbol(StringExpressionSymbols.AND_OPERATOR); }

  [oO][rR] | "||"			{ return symbol(StringExpressionSymbols.OR_OPERATOR); }

  [tT][rR][uU][eE]			{ return symbol(StringExpressionSymbols.TRUE); }

  [fF][aA][lL][sS][eE]		{ return symbol(StringExpressionSymbols.FALSE); }

  [nN][oO][tT] | \!    		{ return symbol(StringExpressionSymbols.NOT_OPERATOR); }

  "("		{ return symbol(StringExpressionSymbols.OPENPAREN); }

  ")"		{ return symbol(StringExpressionSymbols.CLOSEPAREN); }

  {Identifier}						{ return symbol(StringExpressionSymbols.ATTRIBUTE, yytext()); }

  {DecIntegerLiteral} 				{ long i;
 									  try {
 										i = Long.decode(yytext());
 									  } catch (Exception e) {
 									  	return symbol(StringExpressionSymbols.error,yytext());
 									  }
 									  return symbol(StringExpressionSymbols.INTEGER, new Long(i)); 
 									}
 
 {Double}							{double d;
 									  try {
 										d = new Double(yytext());
 									  } catch (Exception e) {
 									  	return symbol(StringExpressionSymbols.error,yytext());
 									  }
 									  return symbol(StringExpressionSymbols.DOUBLE, new Double(d)); 
 									}
 
 
  {String1} | {String2}				{ 
									  String v = yytext();
									  int l = v.length()-1;
									  return symbol(StringExpressionSymbols.STRING, v.substring(1,l));
									}
  
  \s								{ /* ignore */}
  
  .									{ return symbol(StringExpressionSymbols.error); }
  
}
