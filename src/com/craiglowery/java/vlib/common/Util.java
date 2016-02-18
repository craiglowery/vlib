package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;
import javax.naming.InitialContext;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.craiglowery.java.vlib.common.U_Exception.ERROR;

/**
 * Utility functions.
 *
 */
public class Util {

	private static String videoExtensionsRegex;
	private static java.util.regex.Pattern videoExtensionsPattern = null;
	private static String externalInodeCommand=null;
	private static String vlibTrashDirectory=null;
	private static String vlibTempDirectory=null;
	private static String externalChecksumProgram=null;
	private static boolean initialized=false;
	
	private final static String JNDI_configurationFile = "java:global/configurationFile";

	private static Pattern PATTERN_JAVA_IDENTIFER = null; 


	@PostConstruct
	public static void initialize() {
		System.err.println("----COMMON INITIALIZATION----\n");
		if (initialized)
			return;
		File f = null;
		try {
			U_Exception.initialize();
			String configurationFile = (String) new InitialContext().lookup(JNDI_configurationFile);
			if (configurationFile==null) {
				throw new Exception("configurationFile not specified - check JNDI global binding for "+JNDI_configurationFile);
			}
			f = new File(configurationFile);
			if(!f.isFile())
				throw new Exception(String.format("Configuration file '%s' not found - see JNDI global binding for '%s'", 
						configurationFile,JNDI_configurationFile));
			Config.initialize(configurationFile);
			Crypto.initialize();
			videoExtensionsRegex = Config.getString(ConfigurationKey.VIDEO_EXTENSIONS_REGEX);
			try {
				videoExtensionsPattern = java.util.regex.Pattern.compile(videoExtensionsRegex);
			} catch (Exception e) {
				throw new U_Exception(ERROR.ConfigurationError,
						String.format("regex value of %s (%s) does not compile",
								ConfigurationKey.VIDEO_EXTENSIONS_REGEX.name(),
								videoExtensionsRegex),
						e);
			}
			

			vlibTrashDirectory = Config.getString(ConfigurationKey.SUBDIR_REPO_TRASH);
			if (!directoryExists(vlibTrashDirectory))
				throw new Exception("Trash directory '"+vlibTrashDirectory+"' does not exist or is unreadable");

			vlibTempDirectory = Config.getString(ConfigurationKey.SUBDIR_REPO_TEMP);
			if (!directoryExists(vlibTempDirectory))
				throw new Exception("Temp rectory '"+vlibTempDirectory+"' does not exist or is unreadable");

			f = File.createTempFile("temp", "test");
			java.io.FileOutputStream fo = new FileOutputStream(f);
			fo.write(0);
			fo.close();
			
			externalInodeCommand     = Config.getString(ConfigurationKey.EXTERNAL_STAT_COMMAND);
			//Test the command by trying to stat the cwd
			stat(f.getAbsolutePath());

			externalChecksumProgram = Config.getString(ConfigurationKey.EXTERNAL_CHECKSUM_COMMAND);
			//Test the command by trying to compute a checksum
			computeChecksum(f.getAbsolutePath());
			
		} catch (Exception e) {
			L.log(L.E, "Config-static-initializer", 
					"FATAL ERROR: Configuration of the Util common static class failed: '%s'",
					e.getMessage());
			System.exit(1);
		} finally {
			if (f!=null)
				f.delete();
		}
		
		PATTERN_JAVA_IDENTIFER =Pattern.compile("\\s*([A-Za-z_$][A-Za-z0-9_$]*)\\s*");
		initialized=true;
	}
	
	/**
	 * Safely returns a string value representation of the object referenced, even if the reference is null.
	 * @param s
	 * @return <code>s.toString()</code>, or &quot;&quot; if <code>s</code> is null.
	 */
	public static String nullWrap(Object s) {
		return s==null?"":s.toString();
	}

	/**
	 * Safely returns a string value representation of the object referenced, even if the reference is null.
	 * @param s
	 * @return <code>s.toString()</code>, or &quot;&quot; if <code>s</code> is null.
	 */
	public static String nullWrapShow(Object s) {
		return s==null?"(null)":s.toString();
	}

	/**
	 * Attempts to rename a file, creating directories if necessary.  Both <code>from</code> and <code>to</code> 
	 * paths should resolve to the same filesystem device.
	 * 
	 * @param from  A path name that currently resolves to the file.
	 * @param to    The new path name which will replace <code>from</code>.
	 * @return True if the rename was successful
	 */
	public static void renameFile(File f_from, File f_to) 
		throws Exception 
	{
		try {
			if (f_from==null || f_to==null)
				throw new Exception("both from and to paths must be non-null");
			
			//Ensure any enclosing parent directories of the destination exist
			File f_parent_to = f_to.getParentFile();
			if (f_parent_to!=null && !f_parent_to.exists())
				f_parent_to.mkdirs();
	
			//Rename the from file to the to file location
			if (!f_from.renameTo(f_to))
				throw new Exception("Could not rename file '"+f_from.getAbsolutePath()+"' to '"+
							f_to.getAbsolutePath()+"' - no exception was thrown");
		} catch (Exception e) {
			throw new Exception("File rename error",e);
		}
		
	}
	
	/**
	 * Moves the specified file to the configured trash directory
	 * @param f The file to move to the recycle bin.
	 * @return The name of the file's new location in the recycle bin.
	 * @throws U_Exception with error code FileRenameFailed if the rename fails.
	 */
	public static String moveToTrash(File f) 
		throws U_Exception 
	{
		String trashName = "" + java.util.UUID.randomUUID().toString()+"_"+f.getName();
		File trashDirectory = new File(vlibTrashDirectory);
		File trashFile = new File(trashDirectory,trashName);
		if (!f.renameTo(trashFile))
			throw new U_Exception(U_Exception.ERROR.FileRenameFailed,
					String.format("'%s' to '%s'",f.getAbsolutePath(),trashFile.getAbsolutePath()));
		return trashFile.getAbsolutePath();
	}
	
	/**
	 * Generates a title based on a filename by removing
	 * file extensions, path prefix, and odd characters.
	 *
	 * @param filename The filename from which to derive the title.
	 * @return         The derived title
	 */
	public static String deriveTitle(String filename) {
		File f = new File(filename);
		filename = f.getName();
		//Repeatedly chop off up to 3 suffixes
		String suffix;
		int limit=3;
		while ( (suffix=endsWithKnownVideoExtension(filename)) != null && limit-->0 ) {
			filename = filename.substring(0, filename.length()-suffix.length());
		}
		return filename;
	}

	/**
	 * Returns the file extension with leading period if the extension is one of the
	 * recognized video extensions configured in the configuration file for 
	 * key VIDEO_EXTENSIONS_REGEX.
	 * 
	 * @param filename The filename to test.
	 * @return A string which is the found extension, or <code>null</code> if no matching
	 *        extension was found.
	 */
	public static String endsWithKnownVideoExtension(String filename)
		 {
		try {
			java.util.regex.Matcher m = videoExtensionsPattern.matcher(filename);
			if (m.find()) {
				return m.group(0);
			}
		} catch (Exception e) { /* ignore */ }
		return null;
	}
	
	/**
	 * Used in conjunction with the stat() method.
	 */
	public static class StatBuf {
		public long inode;
		public long linkcount;
	}
	
	/**
	 * Calls an external program, usually stat(1), to retrieve *NIX specific information for a file, namely
	 * the inode number and the hard link count.
	 * 
	 * @param filename Path to the file to stat.
	 * @return A StatBuf object with retrieved file-specific information.
	 */
	public static StatBuf stat(String filename) 
		throws Exception 
	{

		StatBuf buf = new StatBuf();
		try {
			String cmd = String.format("%s '%s'", externalInodeCommand, filename);
			List<String> cmdparms = tokenizeAsShellWould(cmd);
			String[] cmdarray=cmdparms.toArray(new String[cmdparms.size()]);
			String line = runExteranalProgram(cmdarray, 20);
			String[] tokens = tokenizeAsShellWould(line).toArray(new String[0]);
			if (tokens.length!=2)
				throw new Exception("output in unrecognized format\n");
			buf.inode = Long.decode(tokens[0]);
			buf.linkcount =	Long.decode(tokens[1]);
		} catch (Exception e) {
			throw new Exception("External stat query failed for "+filename,e);					
		} 
		return buf;
	}
	
	public static String drainInputStream(InputStream s, int maxchars) throws IOException {
		StringBuffer sb = new StringBuffer(maxchars);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(s));
		String l=null;
		while ( (l = reader.readLine()) != null) {
			if (sb.length()>0)
				sb.append("\n");
			sb.append(l);
			if (sb.length()>=maxchars) {
				sb.setLength(maxchars);
				break;
			}
		}
		return sb.toString();
	}
	
	public static List<String> tokenizeAsShellWould(String s) {
		final int ST_START = 0;
		final int ST_WORD = 1;
		final int ST_SINGLE = 2;
		final int ST_DOUBLE = 3;
		
		ArrayList<String> results = new ArrayList<String>();
		StringBuilder sb = null;
		int state = ST_START;
		for (char c : s.toCharArray()) {
			switch (state) {
			case ST_START:
				if (Character.isWhitespace(c)) continue;
				if (c=='\'') { 
					state=ST_SINGLE;
					sb = new StringBuilder();
					continue; 
				}
				if (c=='\"') { 
					state=ST_DOUBLE;
					sb = new StringBuilder();
					continue; 
				}
				sb = new StringBuilder();
				sb.append(c);
				state=ST_WORD;
				continue;
			case ST_WORD:
				if (Character.isWhitespace(c)) {
					results.add(sb.toString());
					sb=null;
					state = ST_START;
					continue;
				}
				sb.append(c);
				continue;
			case ST_SINGLE:
				if (c=='\'') {
					results.add(sb.toString());
					sb=null;
					state=ST_START;
					continue;
				}
				sb.append(c);
				continue;
			case ST_DOUBLE:
				if (c=='\"') {
					results.add(sb.toString());
					sb=null;
					state=ST_START;
					continue;
				}
				sb.append(c);
				continue;
			}
		}
		if (sb!=null)
			results.add(sb.toString());
		return results;
	}
	
	/**
	 * Runs an external program and returns a {@code String} of the standard output stream.
	 * 
	 * @param cmdarray An array of {@code String} values, the first of which is the command to
	 * execute, with subsequent array values being the arguments to be passed to the command.
	 * 
	 * @return The resulting stdout.  Output of the command may be truncated if it 
	 * is longer than an internally set maximum value.
	 * 
	 * @throws Exception which includes the content of the standard error (if there is any) as part of
	 * the exception message.
	 */
	public static String runExteranalProgram (String[] cmdarray, int maxoutput) 
		throws Exception 
	{
		String pgname = (cmdarray.length>0 && cmdarray[0]!=null) ? cmdarray[0] : "(null)";
		String output=null;
		try {
			Process p = Runtime.getRuntime().exec(cmdarray);
			try (InputStream in = p.getInputStream();
				 InputStream err = p.getErrorStream()){
				output=drainInputStream(in, maxoutput);
				p.waitFor();
				if (p.exitValue()!=0) {
					String erroroutput = drainInputStream(err, 200);
					throw new Exception(String.format("Execution of external program '%s' failed: '%s'",
							pgname, erroroutput));
				}
			}
		} catch (Exception e) {
			throw new Exception(String.format("Execution of external program '%s' failed",pgname),e);					
		}
		return output==null?"":output;
	}
	
	/**
	 * Determines if a string represents a valid hexadecimal number.
	 * 
	 * @param s The string to be tested.  It should NOT include hex prefixes or suffixes such as 0x or H
	 * @return True if the every letter is a valid hexit (matches the re <code>[0-9a-fA-F]+</code>)
	 */
	public static boolean isHexidecimalString(String s) {
		if (s.length()==0)
			return false;
		for (char c : s.toCharArray())
			if (! (( c>='0' && c<='9') || (c>='a' && c<='f') || (c>='A' && c<='F')))
				return false;
		return true;
	}
	
	/**
	 * Computes the SHA1 checksum of a file on disk using an external program .
	 * 
	 * @param f A <code>File</code> object associated with the disk file to be checked
	 * @return A string representation of the SHA1 checksum, with any letter hexits in lower case
	 */
	public static String computeChecksum(String filename) 
			throws Exception 
	{
		try {
			String cmd = String.format("%s '%s'", externalChecksumProgram,filename);
			String[] cmdarray = tokenizeAsShellWould(cmd).toArray(new String[0]);
			String line = runExteranalProgram(cmdarray, 50);
			String[] tokens = tokenizeAsShellWould(line).toArray(new String[0]);
			if (tokens.length<1)
				throw new Exception("wrong number of tokens output from checksum program: "+line);
			String sha1 = tokens[0].toLowerCase();
			if (sha1.length()!=40)
				throw new Exception("token should be 40 characters long");
			if (!isHexidecimalString(sha1))
				throw new Exception("invalid hexidecimal characters");
			return sha1;
		} catch (Exception e) {
			throw new Exception(String.format("Checksum computation of '%s' failed",filename==null?"(null)":filename),e);
		}
	}


	public static boolean isValidFilename(String filename) 
			{
			//1. Must be non-empty string
			//2. Must not begin or end with spaces
			//3. If there are internal spaces, no more than one contiguously
			//4. Any printable characters ASCII value 32 or above except <>:"/\|?*
			
			char[] ca = filename.toCharArray();
			if (ca.length==0) return false;
			if (ca[0]==' ') return false;
			
			boolean sawspace=false;
			for (int x=0; x<ca.length; x++) {
				char c=ca[x];
				if (c==' ') {
					if (sawspace)
						return false;
					sawspace=true;
				} else
					sawspace=false;
				if (c<=31 || c>126 || c=='<' || c=='>' || c==':' || c=='"' || c=='/' || c=='\\' || c=='|' || c=='?' || c=='*')
					return false;
			}
			if (sawspace) return false;
			return endsWithKnownVideoExtension(filename)!=null;
		}
		

	
	/**
	 * Reads count bytes from stream. If necessary, it will perform multiple
	 * reads, sleeping intermittently, until either the number of bytes
	 * required has been read, or end of file or exception are thrown.
	 * 
	 * @param in The <code>InputStream</code> from which to read.
	 * @param buff The buffer in which to place the read contents.
	 * @param count The number of bytes to read in total.
	 * @return The number of bytes actually read
	 * @throws IOException
	 */
	public static int readAtLeast(java.io.InputStream in, byte[] buff, int count)
		throws java.io.IOException
	{
		int bytesread = 0;
		while (count>0) {
			int thisread = in.read(buff,bytesread,count);
			if (thisread<0)  // End of file - that's all we can read!
				break;
			bytesread += thisread;
			count -= thisread;
		}
		return bytesread;
	}
	
	/**
	 * Returns a filename with characters drawn from a limited set.
	 * If a character is not in the set of alphabetic characters and
	 * decimal digits or the special characters {@code -_.} then it
	 * is replaced with an underscore.
	 * @param name The name to be sanitized.
	 * @return The sanitized name.
	 */
	public static String sanitizedFilename(String name) {
		char[] buf = name.toCharArray();
		for (int x=0; x<buf.length; x++) {
			if (Character.isAlphabetic(buf[x])) continue;
			if (Character.isDigit(buf[x])) continue;
			if (buf[x]=='-' || buf[x]=='_' || buf[x]=='.') continue;
			buf[x]='_';
		}
		return new String(buf);
	}
	
	/**
	 * Determines if a file exists at the provide path.
	 * @param path
	 * @return True if the file exists.
	 */
	public static boolean fileExists(String path) {
		File f = new File(path);
		return  f.isFile();
	}
	
	/**
	 * Determines if a directory exists at the provide path.
	 * @param path
	 * @return True if the directory exists.
	 */
	public static boolean directoryExists(String path) {
		File f = new File(path);
		return  f.isDirectory();
	}
	

	
	/**
	 * Determines if a string is composed of characters between ASCII value
	 * 32 (space) and 126 (~, tilde).
	 * @param s The string to be checked.
	 * @return True if the string comprises only characters in the ASCII range 32..126.
	 */
	public static boolean isPrintableCharacters(String s) {
		for (char c : s.toCharArray()) {
			if (c<32 || c>126)
				return false;
		}
		return true;
	}

	/**
	 * <p>Parses a string for all reasonable forms of a "true" or "false" value. Comparison is
	 * not case senstive:</p>
	 * <UL>
	 *   <li><b>TRUE</b>: any one of {@code "true", "1", "on", "yes", "t"}
	 *   <li><b>FALSE</b>: any one of {@code "false", "0","off", "no", "f"}
	 * </UL>
	 * <p>U_Exception error codes:</p>
	 * <UL>
	 *   <li><b>BadParameter</b> - {@code s} is null or does not parse as boolean.
	 * </UL>
	 * @param s The string to parse.
	 * @return The boolean value of s.
	 * @throws U_Exception
	 */
	public static boolean parseBoolean(String s) throws U_Exception {
		if (s==null)
			throw new U_Exception(ERROR.BadParameter,"Boolean string cannot be null");
		String S=s.trim().toLowerCase();
		switch (S) {
		case "true":
		case "1":
		case "on":
		case "yes":
		case "t":
			return true;
		case "false":
		case "0":
		case "off":
		case "no":
		case "f":
			return false;
		default:
			throw new U_Exception(ERROR.BadParameter,String.format("Unparseable boolean string: '%s", s));
		}
	}
	
	  public static void copyStreamToFileAtPosition(final InputStream input, File f, long offset) 
		  throws U_Exception {
		  	try (
		  		final ReadableByteChannel src = Channels.newChannel(input);
	  			final SeekableByteChannel dest = Files.newByteChannel(f.toPath(),EnumSet.of(StandardOpenOption.WRITE));
		  	){
		  		dest.position(offset);
		  		final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);
			    while (src.read(buffer) != -1) {
			      // prepare the buffer to be drained
			      buffer.flip();
			      // write to the channel, may block
			      dest.write(buffer);
			      // If partial transfer, shift remainder down
			      // If buffer is empty, same as doing clear()
			      buffer.compact();
			    }
			    // EOF will leave buffer in fill state
			    buffer.flip();
			    // make sure the buffer is fully drained.
			    while (buffer.hasRemaining()) {
			      dest.write(buffer);
			    }
		    } catch (IOException e) {
		    	throw new U_Exception(ERROR.IOError,e);
		    }
		  }
	
	  /** 
	   * Loads an XML document from the input stream and returns the {@code Document} object.
	   * @param input An input stream from which the XML is to be read.
	   * @return An XML {@code Document}
	   * @throws U_Exception
	   */
	  public static Document buildXmlFromInput(InputStream input) throws U_Exception {
		try {
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			dbf.setValidating(false);
			dbf.setIgnoringComments(true);
			DocumentBuilder db = dbf.newDocumentBuilder();
			return db.parse(input);
		} catch (ParserConfigurationException | SAXException e) {
			 throw new U_Exception(ERROR.ParserError);
		} catch (IOException e) {
			throw new U_Exception(ERROR.IOError);
		}
	  }
	  

	    static private Pattern RE_SEQUENCE_INTEGER_LETTER = Pattern.compile("^(\\d+)([a-z])$");
	    static private Pattern RE_SEQUENCE_INTEGER_DASH_INTEGER = Pattern.compile("^(\\d+)-(\\d+)$");

	    /**
	     * Normalizes strings to a standard sequencing format.
	     *
	     * <UL>
	     *     <LI>All white space is trimmed first.</LI>
	     *     <LI>If it parses as an integer, then it is reformatted using Integer.toString().</LI>
	     *     <LI>If it parses as an integer followed by a letter, then it is reformatted
	     *         as Integer.toString() followed by the character in lower case.</LI>
	     *     <LI>If it parses as a Roman numeral, then it is normalized to upper case
	     *         canonical Roman numeral format. Only the numbers 1 to 10 are
	     *         supported.</LI>
	     *     <LI>If it is a single alphabetic letter, it is converted to lower case.</LI>
	     *     <LI>The original trimmed string is returned if none of the above apply.</LI>
	     * </UL>
	     * @param s
	     * @return
	     */
	    public static String sequencify(String s) {
	        String original=s;
	        s=s.trim().toLowerCase();

	        try {
	            int i = Integer.parseInt(s);
	            return Integer.toString(i);
	        } catch (NumberFormatException e) {}


	        Matcher m = RE_SEQUENCE_INTEGER_LETTER.matcher(s);
	        if (m.matches()) {
	            try {
	              return Integer.toString((Integer.parseInt(m.group(1)))) +  m.group(2);
	            } catch(NumberFormatException e) {}
	        };

	        if (s.length()==1 &&  Character.isAlphabetic(s.charAt(0)))
	            return s;

	        switch (s) {
	            case "i":
	            case "ii":
	            case "iii":
	            case "iv":
	            case "v":
	            case"vi":
	            case "vii":
	            case "viii":
	            case "ix":
	            case "x":
	                return s.toUpperCase();
	        }
	        return original.trim();
	    }

	    /**
	     * Returns an integer value assigned as the sort order for a sequence string as follows:
	     *
	     * <OL>
	     *     <LI>If it parses as a single lower-case alphabetic character, then it gets the value
	     *     from 0 to 25, where 0=a, 1=b, ...</LI>
	     *     <LI>If it parses as an integer, then it is the value parsed shifted left 9 bits.</LI>
	     *     <LI>If it parses as an integer followed by an upper or lower case letter, then
	     *     the integer and letter values are placed into their respective fields as above, and
	     *     upper case letters are treated the same as lower case letters.</LI>
	     *     <LI>If the sequence is a Roman numeral from 1 to 10, meaning it is UPPER CASE
	     *     "I".."X" then it is assigned the integer 1..10 shifted left 5 bits.
	     *     <LI>If it conforms to the pattern "integer-integer" as in "127-128" then
	     *     the first integer in the list is returned.</LI>
	     *     <LI>If it is a sequence of 10 or fewer letters and digits, then a value that will
	     *     sort in the same order as a case-insensitive string comparison will be returned.</LI>
	     *     <LI>Anything else is give the value Long.MAX_VALUE</LI>
	     * </OL>
	     *
	     *
	     <PRE>
	     __ __ __     __ __ __ __ __ __ __ __ __ __ __
	     63 62 61 ... 10 09 08 07 06 05 04 03 02 01 00
	     __ _______________ ___________ ______________
	      0    integer         roman       letter
	          unsigned      1=I .. 10=X   0=a...25=z

	     __   __ __ __ __ __ __   __ __ __ __ __ __   ...   __ __ __ __ __ __    __ __ __
	     63   62 61 60 59 58 57   56 55 54 53 52 51         08 07 06 05 04 03    02 01 00
	     __   _________________   _________________   ...   _________________    ________
	      1   first character      second character         tenth character       unused
	                                    0=a..25=z,26='0'..35='9'
	     </PRE>
	     * @return
	     */
	    public static long sequenceSortOrder(String s) {
	        s = s.trim().toLowerCase();

	        try {
	            int i = Integer.parseInt(s);
	            return i << 9;
	        } catch (NumberFormatException e) {
	        }

	        Matcher m = RE_SEQUENCE_INTEGER_LETTER.matcher(s);
	        if (m.matches()) {
	            try {
	                int i = Integer.parseInt(m.group(1));
	                int c = m.group(2).charAt(0) - 'a';
	                return (i << 9) + c;
	            } catch (NumberFormatException e) {
	            }
	        }

	        m = RE_SEQUENCE_INTEGER_DASH_INTEGER.matcher(s);
	        if (m.matches()) {
	            try {
	                return Long.parseLong(m.group(1)) << 9;
	            } catch (NumberFormatException e) {}
	        }

	        if (s.length() == 1 && Character.isAlphabetic(s.charAt(0)))
	            return s.charAt(0) - 'a';

	        switch (s) {
	            case "i":
	                return 1 << 5;
	            case "ii":
	                return 2 << 5;
	            case "iii":
	                return 3 << 5;
	            case "iv":
	                return 4 << 5;
	            case "v":
	                return 5 << 5;
	            case "vi":
	                return 6 << 5;
	            case "vii":
	                return 7 << 5;
	            case "viii":
	                return 8 << 5;
	            case "ix":
	                return 9 << 5;
	            case "x":
	                return 10 << 5;
	        }

	        if (s.length() <= 10) {
	            long val= 1L << 63;
	            char sarr[] = s.toCharArray();
	            int sarrlen = s.length();
	            char c;
	            long component;
	            for (int x = 0; x<sarrlen; x++) {
	                c = sarr[x];
	                if (c>='a' && c<='z') {
	                    component=c-'a';
	                } else if (c>='0' && c<='9') {
	                    component=c-'0'+26;
	                } else
	                    return Long.MAX_VALUE;
	                val = val | (component << (  ((9-x)*6) + 3));
	            }
	            return val;
	        }

	        return Long.MAX_VALUE;
	    }

	    /**
	     * Appends a value to a {@code StringBuilder} object, inserting a 
	     * spacer between
	     * any previous material and the new value.
	     * @param sb The {@code StringBuilder} to append to.
	     * @param spacer The string to put between joined strings.
	     * @param value The string value to append.
	     */
	    public static void spacerAppend(StringBuilder sb, String value, String spacer) {
	    	if (sb.length()>0)
	    		sb.append(spacer);
	    	sb.append(value);
	    }
	    

	    /**
	     * Appends a value to a {@code StringBuilder} object, insert a comma between
	     * any previous material and the new value.
	     * @param sb The {@code StringBuilder} to append to.
	     * @param value The string value to append.
	     */
	    public static void commaAppend(StringBuilder sb, String value) {
	    	spacerAppend(sb,value,", ");
	    }

	    /**
	     * Returns a string with is the value of one string followed by a spacer value and
	     * the value of another string, if the first string is non-null and non-empty.
	     * Otherwise, it simply returns the second string.
	     * @param onto The string which is the prefix of the resulting string.  If this
	     * string is null or empty, then the result will be the {@code value} string
	     * with no comma preceding it.
	     * @param value The string which is the suffix of the resulting string.  
	     * @param spacer The string to put between joined strings.
	     * @return The value {@code onto+", "+value} if {@code onto} is non-null and not
	     * empty, otherwise {@code value}.
	     */
	    public static String spacerAppend(String onto, String value, String spacer) {
	    	if (onto==null || onto.length()==0)
	    		return value;
	    	return onto+spacer+value;
	    }

	    /**
	     * Returns a string with is the value of one string followed by a comma and
	     * the value of another string, if the first string is non-null and non-empty.
	     * Otherwise, it simply returns the second string.
	     * @param onto The string which is the prefix of the resulting string.  If this
	     * string is null or empty, then the result will be the {@code value} string
	     * with no comma preceding it.
	     * @param value The string which is the suffix of the resulting string.  If
	     * @return The value {@code onto+", "+value} if {@code onto} is non-null and not
	     * empty, otherwise {@code value}.
	     */
	    public static String commaAppend(String onto, String value) {
	    	if (onto==null || onto.length()==0)
	    		return value;
	    	return onto+", "+value;
	    }
	    
	    public static String[] parseIdentifierList(String s) throws U_Exception {
	    	if (s==null || (s=s.trim()).equals("")) 
	    		return new String[0];
	    	String[] identifiers = s.split(",");
	    	for (int x=0; x<identifiers.length; x++) {
	    		Matcher m = PATTERN_JAVA_IDENTIFER.matcher(identifiers[x]);
	    		if (!m.matches())
	    			throw new U_Exception(ERROR.ExpressionError,"malformed attribute list: "+s);
	    		identifiers[x]=m.group(1);
	    	}
	    	return identifiers;
	    }

}
