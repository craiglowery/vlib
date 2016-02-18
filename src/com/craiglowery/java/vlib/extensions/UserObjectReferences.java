package com.craiglowery.java.vlib.extensions;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.craiglowery.java.vlib.common.Config;
import com.craiglowery.java.vlib.common.ConfigurationKey;
import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.repository.VersionsTuple;

/**
 * Installation specific extensions for things like computing
 * alternative references for an object's content.
 *
 */
public class UserObjectReferences {
	
	private static String VLIB_VIDEOS_SAMBA = null;
	private static String VLIB_VIDEOS = null;
	
	public static void insertIntoReferencesElement(VersionsTuple vt, Element referencesElement)throws U_Exception {
		
		if (VLIB_VIDEOS_SAMBA==null) {
			VLIB_VIDEOS_SAMBA=Config.getString(ConfigurationKey.SAMBA_PATH);
			VLIB_VIDEOS=Config.getString(ConfigurationKey.DIR_REPO_ROOT);
			if (VLIB_VIDEOS_SAMBA.length()==0 || VLIB_VIDEOS.length()==0)
				throw new U_Exception(ERROR.ConfigurationError,"VLIB_VIDEOS and/or VLIB_VIDEOS_SAMBA not defined");
		}
		
		Document doc = referencesElement.getOwnerDocument();
		Element ref = doc.createElement("reference");
		ref.setAttribute("mode", "localsamba");
		
		String p = vt.path;
		if (!p.startsWith(VLIB_VIDEOS))
			throw new U_Exception(ERROR.ConfigurationError,"VLIB_VIDEOS does not match prefix of '"+p+"'");
		p = p.replace(VLIB_VIDEOS, VLIB_VIDEOS_SAMBA);
		p = p.replace("/", "\\");
		ref.appendChild(doc.createTextNode(p));
		referencesElement.appendChild(ref);
	}

}
