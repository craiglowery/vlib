package com.craiglowery.java.vlib.common;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
/*
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.craiglowery.java.vlib.common.U_Exception.ERROR;

/**
 * Common configuration subsystem.  Supports key/value pairs in a configuration
 * file being retrieved with typed interpretation:<p>
 * 
 * The configuration file comprises lines that are either configuration
 * directives or comments.  Comments start with a hash sign {@code #}
 * and are ignored.  Configuration lines are of the form:<p>
 * 
 * <pre>
 *     <i>configurationkey</i> =  <i>configurationvalue</i>
 * </pre>
 * 
 * White space to either side of the = sign and at the beginning and end of
 * the line are ignored. The <i>configurationkey</i> is converted to all upper case.<p>
 * 
 * Configuration keys are defined in the {@code ConfigurationKey} enumeration.  In that
 * enumeration definition, one may decorate configuration keys with one of, but not both, optional
 * {@code @RequiredConfigurationKey} and {@code @DefaultConfigurationValue}
 * annotations. They are interpreted as follows:
 * 
 * <dl>
 *   <dt>{@code @RequiredConfigurationKey}
 *   <dd>The key must appear in the configuration file or configuration will fail.
 *   <dt>{@code @DefaultConfigurationValue("default")}
 *   <dd>The key is given the specified default value before the configuration file is
 *   read.  If the key is encountered in the configuration file, then its value will be replaced
 *   with the value from the configuration file.
 *   <dt>no decoration
 *   <dd>The key will return a null value if subsequently referenced.
 * </dl>
 * 
 * After all values have been loaded from the file, the values of all keys are repeatedly scanned
 * looking for a substring (key reference) matching the form {@code {$<i>keyname</i>}, where
 * <i>keyname</i> is the name of a configuration key.  If that key has been defined, then the
 * key reference is replaced with the keys value.  The repeated scanning of the key values
 * continues until either all references have been resolved, or no further progress can be made.
 * In the latter case, and exception is produced by the initialization method.
 * 
 * @author James Craig Lowery
 **/
public class Config {

	private static boolean configured=false;
	
	public static final String CONFIGLINE_RE = "^\\s*([a-zA-Z0-9_]+)\\s*=\\s*(.*)\\s*";
	public static final String COMMENTORBLANK_RE = "^((\\s*)|(\\s*#.*))$";

	/** Caches the string value of each key **/
	static Map<ConfigurationKey,String> stringTable = null;
	
	/*** Caches the boolean value of each key, or null if it can't be interpreted as boolean */
	static Map<ConfigurationKey,Boolean> booleanTable = null;
	
	/*** Caches the integer value of each key, or null if it can't be interpreted as integer */
	static Map<ConfigurationKey,Integer> intTable = null;
	
	/*** Caches the long value of each key, or null if it can't be interpreted as long */
	static Map<ConfigurationKey,Long> longTable = null;
	
	/*** A list of required keys not yet found **/
	static Set<ConfigurationKey> requiredKeysNotYetFound = null;
	
	/*** Quickly converts a string to its enumeration key equivalent */
	static java.util.Hashtable<String,ConfigurationKey> keyLookupTable = null;
	
	/**
	 * Initializes the configuration subsystem.  See documentation for class {@code Config}
	 * for details.
	 * @param configurationFile Path to the configuration file.
	 */
	public static void initialize(String configurationFile) throws U_Exception {
		if (configured)
			return;
		try {
			configured=false;
			// Setup the various arrays
			ConfigurationKey[] keyValues = ConfigurationKey.values();
			keyLookupTable = new Hashtable<String, ConfigurationKey>();
			requiredKeysNotYetFound = new HashSet<ConfigurationKey>();

			stringTable = new EnumMap<ConfigurationKey,String>(ConfigurationKey.class);
			booleanTable = new EnumMap<ConfigurationKey,Boolean>(ConfigurationKey.class);
			intTable =  new EnumMap<ConfigurationKey,Integer>(ConfigurationKey.class);
			longTable =  new EnumMap<ConfigurationKey,Long>(ConfigurationKey.class);
	
			// Traverse the ConfigurationKey enumeration and
			for (ConfigurationKey k : keyValues) {
				// Set up the name lookup array
				keyLookupTable.put(k.name().toUpperCase(), k);
				// Load defaults
				Field f = ConfigurationKey.class.getField(k.name());
				Annotation a = f.getAnnotation(DefaultConfigurationValue.class);
				if (a!=null)
					stringTable.put(k, ((DefaultConfigurationValue)a).value());
				// Collect a list of required keys
				if (f.isAnnotationPresent(RequiredConfigurationKey.class))
					requiredKeysNotYetFound.add(k);
			}
			// Parse the file and 
			//    Load the values encountered
			//    Log a warning for unknown keys
			//    Remove required keys from the list as they are defined
			loadConfig(configurationFile);
			// If some required keys were not found, throw an exception
			if (requiredKeysNotYetFound.size()>0) {
				String[] names = new String[requiredKeysNotYetFound.size()];
				int x = 0;
				for (ConfigurationKey k : requiredKeysNotYetFound)
					names[x++]=k.name();
				throw new U_Exception(ERROR.ConfigurationError,"Required keys not found: "+
						String.join(", ", names));
			}
			//Resolve references
			resolve();
			//Convert to other types where possible
			parse();
			configured=true;
		} catch (U_Exception e) {
			throw e;
		} catch (Exception e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
	}
	
	/**
	 * Determines if the vlib common configuration file was successfully read.
	 * 
	 * @return True if the file was successfully read and a minimal viable configuration loaded.
	 */
	public static boolean isConfigured() {
		return configured;
	}
	
	private static void checkInitialized() throws U_Exception {
		if (!configured)
			throw new U_Exception(ERROR.ConfigurationError,"Configuration not initialized");
	}
	
	/**
	 * Tests to see if s is a valid configuration parameter identifier.
	 * @param s The string to be tested.
	 * @return True if s can be interpreted as a known configuration parameter identifier.
	 */
	public static boolean isConfigurationKey(String s) throws U_Exception {
		checkInitialized();
		return keyLookupTable.containsKey(s.toUpperCase());
	}
	
	/**
     *	
	 * Loads the key/value pair configuration dictionary from the configuration file CONFIGFILE.
	 * Lines in the file of format IDENTIFIER = REST OF LINE creates a key IDENTIFIER and value
	 * "REST OF LINE" - the regular expression for parsing details.  Blank lines and lines
	 * having the first character # are ignored.  Any other lines will cause load failure.  Also,
	 * the config file must contain the minimally viable set of key values.  The current list is:
	 *
	 *	      VLIB_ROOT - points to the library directory, e.g., /pool/videos/lib
	 *
	 * The configuration table will not be loaded and an exception thrown for any of these conditions:
	 * 
	 *    1. A line cannot be interpreted as an identifier assignment or as a comment
	 *    2. The identifier does not map to the case-insensitive <code>name()</code> of a ConfigurationKey enum value
	 *    3. The identifier is encountered more than once in the configuration file.
	 * 
	 * @throws Exception
	 */
	//@SuppressWarnings("resource")
	private static void loadConfig(String configFile) throws U_Exception {
		try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(configFile))) {
			Pattern validline_pat = Pattern.compile(CONFIGLINE_RE);
			Pattern commentorblank_pat = Pattern.compile(COMMENTORBLANK_RE);
			
			int linenumber=0;
			String line;
			Set<ConfigurationKey> keysSeen = new HashSet<ConfigurationKey>();
			while ( (line=br.readLine()) != null ) {
				linenumber++;
				Matcher matcher = validline_pat.matcher(line);
				if (matcher.matches()) {
					String s_key = matcher.group(1).toUpperCase();
					String value = matcher.group(2);
					ConfigurationKey key = keyLookupTable.get(s_key);
					//Is it a known key?
					if (key==null) {
						L.log(L.W, "loadConfig", "Unknown configuration key '%s'", s_key);
						continue;
					}
					//Is it already in the table?
					if (keysSeen.contains(key))
						throw new U_Exception(ERROR.ConfigurationError,"Duplicate key='"+s_key+"' on line "+linenumber+" of "+configFile);
					keysSeen.add(key);
					requiredKeysNotYetFound.remove(key);
					//Populate the strings table
					stringTable.put(key, value);
				} else {
					matcher = commentorblank_pat.matcher(line);
					if (matcher.matches()) {
						
					} else 
						throw new U_Exception(ERROR.ConfigurationError, "Syntax error on line "+linenumber+" of file "+configFile);
				}
			}
		} catch (Exception exception) {
			throw new U_Exception(ERROR.ConfigurationError,"Unable to load configuration",exception);
		} 
	}

	private static void resolve() throws U_Exception {
		 boolean progress=false;
		 boolean unresolved=false;
		 final String RE="(.*)\\{\\$([A-Za-z_]+)\\}(.*)";
		 Pattern pattern = Pattern.compile(RE);
		 int tries=5000;
		 do {
			 unresolved=false;
			 progress=false;
			// 	 Scan all key values
			 for (ConfigurationKey key : stringTable.keySet()) {
				// If a value has a key reference in it
				Matcher m = pattern.matcher(stringTable.get(key));
				if (m.matches()) {
					// Indicate unresolved references
					unresolved=true;
					// If the key is defined
					String s_keyref = m.group(2).toUpperCase();
					ConfigurationKey keyref = keyLookupTable.get(s_keyref);
					if (keyref!=null) {
						// Replace the reference with the value
						String value = stringTable.get(keyref);
						stringTable.put(key, m.group(1)+value+m.group(3));
						// Indicate progress
						progress=true;
					}
				}
			 }
			//   If there were unresolved references but no progress
			 if (unresolved && !progress)
				 // Throw an exception
				 throw new U_Exception(ERROR.ConfigurationError,"Unresolved references");
			 	// Until there are no unresolved references
			 if (--tries==0)
				 throw new U_Exception(ERROR.ConfigurationError,"Resolution not converging - infinite recursion suspected");
		 } while (unresolved);
	}

	private static void parse() {
		for (ConfigurationKey key : stringTable.keySet()) {
			//Populate the boolean table
			String value=stringTable.get(key).toLowerCase().trim();
			if ("|yes|on|true|1|".contains("|"+value+"|"))
				booleanTable.put(key, true);
			else if ("|no|off|false|0|".contains("|"+value+"|"))
				booleanTable.put(key, false);
			try {
				int i = Integer.decode(value);
				intTable.put(key,i);
			} catch (NumberFormatException nfe) {}
			try {
				long l = Long.decode(value);
				longTable.put(key,l);
			} catch (NumberFormatException nfe) {}
		}
	}
	
	/**
	 * Gets the associated value for a key.
	 * @param key The key to use in the value query.
	 * @return The string value associated with key, or the empty string if it is undefined.
	 */
	public static String getString(ConfigurationKey key) throws U_Exception 
	{
		checkInitialized();
		return stringTable.containsKey(key) ? stringTable.get(key) : null;
	}

	/**
	 * Returns the <code>int</code> interpretation of a configuration parameter.
	 * @param key The key of interest.
	 * @return The Integer object holding the int value, or null if it cannot be interpreted as int.
	 */
	public static Integer getInt(ConfigurationKey key)  throws U_Exception
	{		
		checkInitialized();
		return intTable.containsKey(key) ? new Integer(intTable.get(key)) : null;
	}

	/**
	 * Returns the <code>long</code> interpretation of a configuration parameter.
	 * @param key The key of interest.
	 * @return The Long object holding the long value, or null if it cannot be interpreted 
	 * as long.
	 */
	public static Long getLong(ConfigurationKey key)  throws U_Exception
	{		
		checkInitialized();
		return longTable.containsKey(key) ? new Long(longTable.get(key)) : null;
	}



	/**
	 * Returns the <code>long</code> interpretation of a configuration parameter.
	 * @param key The key of interest.
	 * @return The Boolean object holding the boolean value, or null.
	 */
	public static boolean getBoolean(ConfigurationKey key) throws U_Exception 
	{
		checkInitialized();
		return booleanTable.containsKey(key) ? new Boolean(booleanTable.get(key)) : null;
	}

	/**
	 * Creates an {@code configuration} Element owned by the specified document
	 * but not inserted into the document.  The {@code configuration} returned
	 * contains the contents of the current repository configuration settings.
	 * @param doc The document which will own the newly create XML structure.
	 * @return An XML {@code Element} containing the current configuration settings.
	 * @throws U_Exception
	 */
	public static Element getConfigurationXml(Document doc) throws U_Exception {
		checkInitialized();
		Element elConfiguration = doc.createElement("configuration");
		for (ConfigurationKey key : ConfigurationKey.class.getEnumConstants()) {
			try {
				//Some keys are marked "secure", and we never show them or their values via this report!
				Field f = ConfigurationKey.class.getField(key.name());
				if (f.getAnnotation(SecureConfigurationValue.class)!=null)
					continue;
			} catch (Exception e) {
				continue;  //shouldn't happen - we know these fields are in there
			}

			Element elKey = doc.createElement("key");
			elKey.setAttribute("name",key.name());
			elConfiguration.appendChild(elKey);
			Element elValue = doc.createElement("string");
			elValue.appendChild(doc.createTextNode(stringTable.get(key)));
			elKey.appendChild(elValue);
			if (booleanTable.containsKey(key)) {
				elValue = doc.createElement("boolean");
				elValue.appendChild(doc.createTextNode(booleanTable.get(key).toString()));
				elKey.appendChild(elValue);
			}
			if (intTable.containsKey(key)) {
				elValue = doc.createElement("int");
				elValue.appendChild(doc.createTextNode(intTable.get(key).toString()));
				elKey.appendChild(elValue);
			}
			if (longTable.containsKey(key)) {
				elValue = doc.createElement("long");
				elValue.appendChild(doc.createTextNode(longTable.get(key).toString()));
				elKey.appendChild(elValue);
			}
		}
		return elConfiguration;
	}
	
}




