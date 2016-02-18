package com.craiglowery.java.vlib.common;

import java.util.ArrayList;
import java.util.List;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.craiglowery.java.vlib.common.U_Exception.ERROR;

/**
 * XPath helper class.  Provides shortcuts to quickly use common XPath expressions on
 * a document. 
 *
 */
public class XP {

	Document doc;
	XPath xp;
	
	public XP(Document doc) throws U_Exception {
		this.doc=doc;
		xp = XPathFactory.newInstance().newXPath();
	}
	
	public Element el(String path) throws U_Exception {
		if (path==null) 
			path="/";
		Node n = null;
		try {
			n = (Node) xp.compile(path).evaluate(doc, XPathConstants.NODE);
		} catch (XPathExpressionException e) {
			throw new U_Exception(ERROR.ParserError,"Invalid XPath expression: "+path,e);
		}
		if (n==null)
			throw new U_Exception(ERROR.ParserError,"XPath not found: '"+Util.nullWrap(path)+"'");
		if (n.getNodeType()!=Node.ELEMENT_NODE)
			throw new U_Exception(ERROR.ParserError,"Node is not an Element");
		return (Element)n;
	}
	
	public List<Element> els(String path) throws U_Exception {
		if (path==null) 
			path="/";
		NodeList nl = null;
		try {
			nl = (NodeList) xp.compile(path).evaluate(doc, XPathConstants.NODESET);
		} catch (XPathExpressionException e) {
			throw new U_Exception(ERROR.ParserError,"Invalid XPath expression: "+path,e);
		}
		if (nl==null)
			throw new U_Exception(ERROR.ParserError,"XPath not found");
		List<Element> r = new ArrayList<Element>();
		int len = nl.getLength();
		for (int x=0; x<len; x++) {
			Node n = nl.item(x);
			if (n.getNodeType()!=Node.ELEMENT_NODE)
				throw new U_Exception(ERROR.ParserError,"XPath returns some non-Element nodes");
			r.add((Element)n);
		}
		return r;
	}
	
	public String el_text(String path) throws U_Exception {
		Element el = el(path);
		Node n = el.getFirstChild();
		if (n==null) return "";
		if (n.getNodeType()!=Node.TEXT_NODE)
			throw new U_Exception(ERROR.ParserError,"Content of node at "+path+" is non-text");
		return n.getNodeValue();
	}
	
}
