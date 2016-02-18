package com.craiglowery.java.vlib.repository;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.HashMap;

import java.util.TreeSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.craiglowery.java.vlib.common.*;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;
import com.craiglowery.java.vlib.extensions.UserObjectReferences;
import com.craiglowery.java.vlib.filter.ExprException;
import com.craiglowery.java.vlib.filter.ExpressionFactory;
import com.craiglowery.java.vlib.filter.WhereConditionParser;
import com.craiglowery.java.vlib.tuple.PersistenceConnection;
import com.craiglowery.java.vlib.tuple.PostgresqlConnection;
import com.craiglowery.java.vlib.tuple.PostgresqlTableAdapter;
import com.craiglowery.java.vlib.tuple.SelectionTransformer;
import com.craiglowery.java.vlib.tuple.SortDirective;
import com.craiglowery.java.vlib.tuple.SortDirective.Order;
import com.craiglowery.java.vlib.tuple.TableAdapter;
import com.craiglowery.java.vlib.tuple.Tuple;
import com.craiglowery.java.vlib.tuple.filterexp.TupleExpressionFactory;
import com.craiglowery.java.vlib.tuple.filterexp.FilterExpressionException;


/**
 * Provides a local interface for manipulating objects and their content 
 * and meta-data in the repository.  Implements pooling of RM instances,
 * since construction can be resource and time intensive. Use
 * "getRepositoryManager()" method to get an RM instance for use, being
 * sure to use the try using pattern as follows:<p>
 * 
 * <pre>
 *      {@code
 *      try (RepositoryManager rm = RepositoryManager.getRepositoryManager()) {
 *        //..use the rm
 *      }
 * </pre>
 * 
 * If you do NOT use the form above, then be sure to call rm.close() to return
 * the instance to the pool or you will develop a resource leak!
 * 
 * The main purpose of this class is to provide incoming requests an
 * abstracted set of methods that manipulate things in the repository like
 * objects, their versions, and their tags.  It uses a set of TableAdapter
 * derived generic classes to interact with the backend storage, without
 * having to know the actual type of backing mechanism.  In this way,
 * one could implement the repository with ANY persistence system, but
 * would most likely use a SQL database, or a distributed noSQL database
 * for large scale projects.  Be sure to see TableAdapter and Tuple classes
 * for more information on how this abstraction is achieved.
 */
public class RepositoryManager implements AutoCloseable {

//--------static stuff that manages the collection of RM's

	/** Number of seconds from an instance's time of creation to being considered "expired" **/
	private static final int RM_LIFETIME_SECONDS=60*5;
	
	/** The next serial number to be given to a newly constructed instance. **/
	private static Integer serialNumberCounter = 0;   
	
	/** The pool of instances which are reused before new ones are constructed. **/
	private static java.util.ArrayList<RepositoryManager> pool = new java.util.ArrayList<RepositoryManager>();
	
	/** Frequently used Configuration values from the configuration file. **/
	private static String dirRepoLib;
	private static String dirRepoTemp;
	private static boolean classInitialized=false;
	private static U_Exception initializationError=null;
	private static String dataBaseName;
	private static String hostName;
	
	static {
		try {
			initializeClass();
		} catch (U_Exception e) {
			initializationError=e;
		}
	}
	
	/**
	 * Static initializer for the class, called at class load time.  If it fails,
	 * then the private static variable {@code initializationError} is set with
	 * the resulting exception.  
	 * 
	 * @throws U_Exception
	 */
	private static void initializeClass() throws U_Exception {
		dirRepoLib = Config.getString(ConfigurationKey.SUBDIR_REPO_LIB);
		if (dirRepoLib.length()==0)
			throw new U_Exception(U_Exception.ERROR.ConfigurationError,"SUBDIR_REPO_LIB is not configured");

		dirRepoTemp = Config.getString(ConfigurationKey.SUBDIR_REPO_TEMP);
		if (dirRepoTemp.length()==0)
			throw new U_Exception(U_Exception.ERROR.ConfigurationError,"SUBDIR_REPO_TEMP is not configured");

		dataBaseName = Config.getString(ConfigurationKey.DB_NAME);
		if (dataBaseName.length()==0)
			throw new U_Exception(U_Exception.ERROR.ConfigurationError,"DATABASENAME is not configured.");
		hostName = Config.getString(ConfigurationKey.DB_HOSTNAME);
		if (hostName.length()==0)
			throw new U_Exception(U_Exception.ERROR.ConfigurationError,"HOSTNAME is not configured.");

		
		classInitialized=true;
	}
	
	/**
	 * Looks in the pool for an unused repository manager. If it finds one, then
	 * it reuses it, unless it has expired.
	 * @return An instance of {@code RepositoryManager} ready for use by a client.
	 */
	public static synchronized RepositoryManager getRepositoryManager()
		throws U_Exception
	{
			if (initializationError!=null)
				throw initializationError;
			if (!classInitialized)
				initializeClass();
				
			if (U_Exception.logger==null)
				U_Exception.ConfigureLogger();
			// 1. Scan the pool closing and nulling out expired entries, and looking for a not-in-use one
			java.util.LinkedList<RepositoryManager> expiry = new LinkedList<RepositoryManager>();
			RepositoryManager suitable = null;
			for (RepositoryManager rm : pool) {
				if (!rm.inuse) {
					Instant now = Instant.now();
					if (rm.expires.isBefore(now)) {
						// The RM has expired - add it to the expiry list
						expiry.add(rm);
					} else {
						// The RM has not expired and is not in use.  Double check that it is valid.
						if (rm.connection==null || !rm.connection.isValid())
							expiry.add(rm);
						else {
							if (suitable==null) {
								// This is the first suitable, non-expired RM
								suitable=rm;
							}
						}
					}
				}
			}
		
			// If we found any expired RM's, we need to close them out and remove them from the pool
			for (RepositoryManager rm : expiry) {
				pool.remove(rm);
				rm.dispose();
			}
			// If we found no suitable rm, we must construct one and place it in the pool
			if (suitable==null) {
				suitable = new RepositoryManager();
				pool.add(suitable);
			}
			// Mark it as in-use
			suitable.inuse = true;
			return suitable;
	}
	
	/** Returns the next serial number to be assigned to an instance.
	 * @return The next serial number.
	 */
	private static int getNextSerialNumber() {
		synchronized (serialNumberCounter) {
			if (serialNumberCounter==Integer.MAX_VALUE)
				serialNumberCounter=0;
			return ++serialNumberCounter;
		}
	}

	/**
	 * Throws an exception if the object in question is null.
	 * @param obj
	 * @throws U_Exception
	 */
	private static void checkNull(Object obj) throws U_Exception {
		if (obj==null) throw new U_Exception(ERROR.BadParameter);
	}
	
	/**
	 * Creates a report of the current RepositoryManager pool.  Used by administrators
	 * to determine the health and utilization of the pool.
	 * @param doc The {@code Document} that owns the report nodes which will be created.
	 * @return The root node of the report, which must be inserted into {@code doc} a the
	 * appropriate location by the caller.
	 */
	public static Element statusXml(Document doc) {
		Element elRMStatus = doc.createElement("rmstatus");
		LambdaTwoStrings rootadd = (name,value) -> {
			Element el = doc.createElement(name);
			el.appendChild(doc.createTextNode(value));
			elRMStatus.appendChild(el);
		};
		
		rootadd.op("lifetimeseconds",Integer.toString(RM_LIFETIME_SECONDS));
		rootadd.op("serialcounter",Integer.toString(serialNumberCounter));
		
		Element elPool = doc.createElement("pool");
		elPool.setAttribute("size", Integer.toString(pool.size()));
		
		int seq = 0;
		for (RepositoryManager rm : pool) {
			Element elRM = doc.createElement("repositorymanager");
			
			LambdaTwoStrings rmadd = (name,value) -> {
				Element el = doc.createElement(name);
				el.appendChild(doc.createTextNode(value));
				elRM.appendChild(el);
			};
			
			rmadd.op("sequence", Integer.toString(seq++));
			rmadd.op("serialnumber", Integer.toString(rm.serialNumber));
			rmadd.op("expires", rm.expires.toString());
			long ttl = rm.expires.toEpochMilli() - Instant.now().toEpochMilli();
			rmadd.op("timetolive", Long.toString(ttl/1000));
			rmadd.op("inuse", rm.inuse?"yes":"no");
			boolean valid=false;
			try {
				valid=rm.connection!=null && rm.connection.isValid();
			} catch (Exception e) {	}
			rmadd.op("isvalid", valid?"yes":"no");
			
			elPool.appendChild(elRM);
		}
		elRMStatus.appendChild(elPool);
		return elRMStatus;
	}
	
	
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//------instance stuff-----------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
//-------------------------------------------------------------------------------------------
	
	private Random rand = new Random();   // A random number generator for this instance
	
	//Objects that get us access to the backing store
	private PersistenceConnection connection = null;
	private TableAdapter<VersionsTuple> versionsTable = null;
	private TableAdapter<VersionsTuple> trashVersionsTable = null;
	private TableAdapter<ObjectsTuple> objectsTable = null;
	private TableAdapter<TagsTuple> tagsTable = null;
	private TableAdapter<Tag_valuesTuple> tag_valuesTable = null;
	private TableAdapter<Object_tagsTuple> object_tagsTable = null;
	private TableAdapter<Object_tagsTuple> trashObject_tagsTable = null;
	private TableAdapter<VersionsTuple> currentVersionsTable = null;
	private ExpressionFactory versionsTupleExpressionFactory = null; 
	
	/** Unique serial number of this instance in the pool. **/
	final private int serialNumber = getNextSerialNumber();    
	
	/** True if this instance is currently in use (somewhere
	 * between {@code getRepositoryManager()} and {@code close()}. **/
	private boolean inuse;
	
	/** When this instance is considered stale and should be retired **/
	private Instant expires;

	
	/**
	 * Initializes a new instance.
	 */
	private void initialize() 
		throws U_Exception 
	{
		//This should be the only place where you have to make reference to a specific table backing store (Postgres, etc.)
		connection = new PostgresqlConnection(hostName, dataBaseName, "veditor", Crypto.getPassword("veditor", "sd90uj*(3l"));
		versionsTable = new PostgresqlTableAdapter<VersionsTuple>(VersionsTuple.class,connection, "versions", true);
		trashVersionsTable = new PostgresqlTableAdapter<VersionsTuple>(VersionsTuple.class,connection,  "trashversions", true);
		objectsTable = new PostgresqlTableAdapter<ObjectsTuple>(ObjectsTuple.class,connection, "objects", true);
		tagsTable = new PostgresqlTableAdapter<TagsTuple>(TagsTuple.class,connection,"tags",true);
		tag_valuesTable = new PostgresqlTableAdapter<Tag_valuesTuple>(Tag_valuesTuple.class,connection,"tag_values",true);
		object_tagsTable = new PostgresqlTableAdapter<Object_tagsTuple>(Object_tagsTuple.class,connection, "object_tags",true);
		trashObject_tagsTable = new PostgresqlTableAdapter<Object_tagsTuple>(Object_tagsTuple.class,connection, "trashobject_tags",true);
		currentVersionsTable = new PostgresqlTableAdapter<VersionsTuple>(VersionsTuple.class,connection,  "currentversions", false);
		try {
			versionsTupleExpressionFactory = new ExpressionFactory(this, currentVersionsTable);
		} catch (Exception e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,"Could not intialize expression factory",e);
		}
		inuse=false;
		expires=Instant.now().plusSeconds(RM_LIFETIME_SECONDS);
	}
	
	
	
	/**
	 * Creates a new repository manager instance.
	 */
	private RepositoryManager() 
		throws U_Exception
	{
		initialize();
	}

	
	/**
	 * Used by clients to "close" this RM, which actually returns it to the pool of eligible
	 * RM's for other clients.
	 */
	public void close() throws Exception {
		if (!inuse) 
			L.log(L.E,this,"call to close but already marked unused");

		if (connection.transactionInProgress()) {
			connection.rollbackTransaction();
			L.log(L.E,this,"ResourceManager was closed with open transaction");
		}
		inuse=false;
	}
	
	/**
	 * Closes the associated database connection.  Used to REALLY
	 * shut down this RepositoryManager
	 */
	private void dispose() {
		try {
			close();
			connection.close();
		} catch (Exception e) {
			L.log(L.E,this,"unexpected exception thrown during proxy close",e);
			// Ignore
		}
		connection=null;
	}


	
	//----------------------------------------------------------------------------------------
	//-----Object management methods
	//----------------------------------------------------------------------------------------
	
	/**
	 * Generates a new path name for use in storing content in the lib portion of the repository.
	 * There are three levels of content directories beneath lib/, and each level is a letter from
	 * 'a' to 'z'. This allows us to avoid putting all content in a single directory (poor performance
	 * and resiliency), instead spreading content out across potentially 175716 subdirectories.
	 * 
	 * This routine is not static so that each instance can have its own random number generator,
	 * helping to ensure even distribution of files across the directory structure.
	 * 
	 * @param currentFileName  The filename we are trying to find a place for - 
	 * we need the "name" component.
	 * @return The File object having the absolute path to the new proposed location.
	 * @throws U_Exception
	 */
	private File generateNewLibraryLocation(String currentFileName)
		throws U_Exception
	{
		char dist1, dist2, dist3;
		int tries = 30;

		File currentFile = new File(currentFileName);
		String lastPathComponent = currentFile.getName();

		File newFileLocation = null;
		while (tries-- > 0) {
			dist1 = (char) ('a' + rand.nextInt(26)); //Choose a random letter from 'a' to 'z'
			dist2 = (char) ('a' + rand.nextInt(26));
			dist3 = (char) ('a' + rand.nextInt(26));

			newFileLocation = new File(dirRepoLib+"/"+dist1+"/"+dist2+"/"+dist3+"/"+lastPathComponent);
			//If there is no file here, then we will break and return it below.
			//Otherwise, we'll reset to null and try to loop around and try again.
			//If we are out of tries, we'll L.EXIT the loop and fire the exception because newFileLocation==null
			if (!newFileLocation.exists()) 
				break;
			newFileLocation = null;
		}
		if (newFileLocation==null)
			throw new U_Exception(U_Exception.ERROR.Timeout,
				String.format("Tried repeatedly to create a unique permanent filename for '%s' and failed",
						currentFileName));
		return newFileLocation;
	}

	
	/**
	 * Creates a new object in the repository with content copied from local file <code>filename</code>.
	 * 
	 * @param sourceFilename   The local file from which content will be copied
	 * @param suggestedFilename  A suggested filename to be used for this file in the repository.
	 * @param title The title for this content (version). If null or empty, then a title will
	 * 				be derived from {@code suggestedFilename}.
	 * @param duplicateCheck  If true, then the operation will fail if there is an object in the repository
	 *                        with the same length and checksum.
	 * @return The handle of the newly created object.
	 * @throws U_Exception
	 */
	public int createObject(
			String sourceFilename,
			String suggestedFilename,
			String title,
			boolean duplicateCheck)
			throws U_Exception 
	{

		return importObjectContent(0,sourceFilename,suggestedFilename,title,duplicateCheck);
	}
		

	/**
	 * Updates the object with new content copied from local file <code>filename</code>. The updated
	 * content becomes the new current version of the object's content.  The previous content is kept as an
	 * older version that can be accessed or permanently deleted through other calls of the repository manager
	 * interface.
	 * 
	 * @param handle     The handle of an existing object entry in the <code>videos</code> table for which a new
	 *                   version of content is to be imported, or 0 if a new object with a new handle should
	 *                   be created.
	 * @param sourceFilename   The local file from which content will be copied
	 * @param suggestedFilename  A suggested filename to be used for this file in the repository.
	 * @param title The title for this content (version). If null or empty, then a title will
	 * 				be derived from {@code suggestedFilename}.
	 * @param duplicateCheck  If true, then the operation will fail if there is an object in the repository
	 *                        with the same length and checksum.
	 * @return The handle of the object (same as handle passed in).
	 */
	public int updateObject(
			int handle, 
			String sourceFilename, 
			String suggestedFilename, 
			String title, 
			boolean duplicateCheck )
		throws U_Exception 
	{
		return importObjectContent(handle, sourceFilename, suggestedFilename, title, duplicateCheck);
			
	}

	/**
	 * Updates a tuple in the Versions table.
	 * @param vt The tuple to be updated.
	 * @throws U_Exception
	 */
	public void putVersion(VersionsTuple vt) throws U_Exception {
		versionsTable.update(vt);
	}
	
	/**
	 * Performs an import by copying content from <code>filename</code> into the repository 
	 * as a new version of a the object, returning the object’s handle.  This is a workhorse routine that
	 * is called by createObject and updateObject.
	 * 
	 * @param handle     The handle of an existing object entry in the <code>videos</code> table for which a new
	 *                   version of content is to be imported, or 0 if a new object with a new handle should
	 *                   be created.
	 *                   
	 * @param sourceFilename   The local file from which content will be copied
	 * @param suggestedFilename  A suggested filename to be used for this file in the repository.
	 * @param title The title for this content (version). If null or empty, then a title will
	 * 				be derived from {@code suggestedFilename}.
	 * @param duplicateCheck  If true, then the operation will fail if there is an object in the repository
	 *                        with the same length and checksum.
	 *                        
	 * @param trial  If {@code true}, then the function runs normally except nothing is actually imported.
	 *               Typically used to test the import first to see if it will fail.
	 */
	private int importObjectContent(
			int handle,
			String sourceFilename,
			String suggestedFilename,
			String title,
			boolean duplicateCheck)
		throws U_Exception 
	{
		//Sanitize parameters
		if (sourceFilename==null || (sourceFilename=sourceFilename.trim()).equals(""))
			throw new U_Exception(U_Exception.ERROR.InvalidFilename,"sourceFilename");
		if (suggestedFilename==null || (suggestedFilename=suggestedFilename.trim()).equals(""))
			throw new U_Exception(U_Exception.ERROR.InvalidFilename,"suggestedFilename");
		if (title==null || (title=title.trim()).equals(""))
			title=Util.deriveTitle(suggestedFilename);
		
		//See if the source file exists and is a normal file
		java.io.File copiedFrom = new java.io.File(sourceFilename);
		if (!copiedFrom.isFile()) 
			throw new U_Exception(U_Exception.ERROR.NoSuchFile,
					String.format("'%s' does not exist/is not a normal file.",sourceFilename));
		
		//Try to open the file - this try block makes sure the input file gets closed
		try (java.io.FileInputStream in = new java.io.FileInputStream(copiedFrom)){		
		    
			//The input file was successfully opened.  
			
			//Now, follow closely, because here is what we are going to do next:
			//  1. Create a temporary file on the same filesystem as the repository files are stored
			//  2. Copy the sourcefile to this temporary file
			//  3. (deprecated - moved to external prog for perf.) Compute the sha1sum as we are copying
			//  4. If successful, we will link the temporary file into its permanent directory
			//  5. Unlink it from the temporary directory
			
			java.io.File tmpDir = new java.io.File(dirRepoTemp);
			
			//Get a unique temporary file name in VLIB_TMP directory
			java.io.File newContentFileInTemp;
			try {
				newContentFileInTemp = java.io.File.createTempFile("vrm_", ".tmp", tmpDir);
			} catch (Exception e) {
				throw new U_Exception(U_Exception.ERROR.FileError,
						"Could not create temporary file",e);
			}
			
			//Open the temporary file for writing
			try (java.io.FileOutputStream out= new java.io.FileOutputStream(newContentFileInTemp)){
				
				//Perform a copy from the source file to the temp file
				final int BUFFER_SIZE = 256*1024*1024;
				byte[] data = new byte[BUFFER_SIZE];
				int read=0;
				try {
					while ((read = in.read(data)) != -1) {
						out.write(data, 0, read);
						// If we were computing the sha1sum, we'd add to the digest here
					}
					in.close();  //We force a close here, even though the try block will do it later
					out.close(); //This is so we can further manipulate the out file in a closed state
				} catch (IOException ioe) {
					throw new U_Exception(U_Exception.ERROR.IOError,"While copying from source file",ioe);
				}
				
				//Get the sha1sum of the file we just copied
				String sha1sum = null;
				try {
					sha1sum = Util.computeChecksum(newContentFileInTemp.getAbsolutePath());
				} catch (Exception e) {
					throw new U_Exception(U_Exception.ERROR.EncryptionError,"Computing checksum",e);
				}
				//Get the inode number
				Util.StatBuf statBuf = null;
				try {
					statBuf = Util.stat(newContentFileInTemp.getAbsolutePath());
				} catch (Exception e) {
					throw new U_Exception(U_Exception.ERROR.IOError,"Querying for inode number of temporary file",e);
				}
				//
				if (duplicateCheck) {
					int collisionHandle=fingerprintExists(sha1sum,copiedFrom.length());
					if (collisionHandle!=0)
						throw new U_Exception(U_Exception.ERROR.PotentialDuplicate,
							String.format("A version of object %d has the same fingerprint (sha1sum and length) as incoming content '%s'",
									collisionHandle,sourceFilename));
				}
				
				//Determine a permanent file name
				File newContentFileFinalPath = generateNewLibraryLocation(suggestedFilename);
				
				//Construct a new version record
				VersionsTuple vt = new VersionsTuple();
				
				
				vt.handle=(handle);
				vt.path=(newContentFileFinalPath.getAbsolutePath());
				vt.sha1sum=(sha1sum);
				vt.length=(newContentFileInTemp.length());
				vt.title=title;
				vt.copiedfrom=(copiedFrom.getAbsolutePath());
				vt.inode=(statBuf.inode);
				
				addVersion(vt,
						//This is a Lambda code block, being passed as a parameter to addVersion.	
						//It will be called before addVersion commits the new database changes.
						//It returns if only if the rename is successful.
						//Otherwise it throws an U_Exception (causing rollback).
						() -> {
							try {
								Util.renameFile(newContentFileInTemp,newContentFileFinalPath);
							} catch (Exception e) {
								throw new U_Exception(U_Exception.ERROR.FileRenameFailed,e);
							}
						});

				handle=vt.handle;

			} catch (FileNotFoundException e) {
				throw new U_Exception(U_Exception.ERROR.NoSuchFile,e);
			} finally { // --ensures the output file has been closed and deleted if still existent in the temp directory
				try {
					newContentFileInTemp.delete();
				} catch (Exception e) { 
					/*ignore*/
				}	
			}
			//--End of FileOutpuStream try block
		} catch (IOException e1) {
			throw new U_Exception(U_Exception.ERROR.IOError,e1);
		} 
		//--End of FileInputStream try block
		
		return handle;
	}

	/**
	 * Removes an older version of the object and moves its contents to trash.
	 * This operation differs from rolling back a version because no metadata
	 * is changed. Rollback would remove the current version and restore next most
	 * recent version as the current version.
	 * 
	 * @param handle The handle of the object to retire.  
	 * @param imported The timestamp of the version to retire, which MUST NOT be the
	 *        current (most recent) version. 
	 */
	public void retireObjectVersion(int handle, Instant imported) 
		throws U_Exception 
	{
		if (imported==null)
			throw new U_Exception(U_Exception.ERROR.BadParameter,"imported cannot be null");
		
		//First, let's get the object record for this handle, so we know that a) it exists and
		// b) what its most recent import date (latest version) SHOULD be.
		ObjectsTuple ot = getObject(handle);
		
		//There are no versions of this object
		if (ot==null)
			throw new U_Exception(U_Exception.ERROR.NoSuchVersion);

		//Now lets get all the version tuples for this object, with the most recent first
		int found=-1;
		List<VersionsTuple> lvt = getVersions(handle);
		//Is the version being requested in the array?
		for (int x=0; x<lvt.size(); x++)
			if (lvt.get(x).imported.equals(imported)) {
				found=x;
				break;
			}
		
		//Did we find it?
		if (found==-1) {  
			throw new U_Exception(U_Exception.ERROR.NoSuchVersion,"Handle "+handle+" at "+imported.toString());
		}
		
		//Are we being asked to retire the most recent version? 
		if (found==0) // We've been asked to retire the current version
			throw new U_Exception(U_Exception.ERROR.IllegalRequest,"Cannot retire the current version of an object");
		
		trashObjectVersion(handle, imported, null);
	}
	
	
	/**
	 * Rolls back the object to the most recent version, deleting the current version.  The "most recent version" is
	 * simply the version in the {@code versions} table that has the next most recent imported date. It may not
	 * be in-fact the prior version if that version was retired since the content was updated. 
	 * 
	 * If there is only one version then nothing changes, but the function reports a rollback timestamp just the same 
	 * and is considered successful.
	 * 
	 * @param handle  The handle of the object to roll back.
	 * @return The time stamp value of the now-current version.
	 */
	public Instant rollbackObjectToPreviousVersion(int handle) 
		throws U_Exception 
	{
		
		/*
		 * 1. Get the object master record by handle to confirm existence.
		 * 2. Get a list of all versions, ordered descending by import timestamp
		 * 3. Validate that the master record import time matches that of the first in the list (consistency)
		 * 4. If there is more than one version record
		 * 5.    Get the imported date of the next-most recent version
		 * 6.    Trash the current version using the option to update the imported timestamp
		 * 7. Return the current version timestamp
		 */

		//Get master record
		ObjectsTuple ot = getObject(handle);
		if (ot==null)
			throw new U_Exception(ERROR.NoSuchHandle);
		 
		//Get list of all versions
		List<VersionsTuple> lvt = getVersions(handle);
		if (lvt==null || lvt.size()==0)
			throw new U_Exception(ERROR.NoSuchVersion);
		
		//Check consistency
		if (!lvt.get(0).imported.equals(ot.imported))
			throw new U_Exception(U_Exception.ERROR.InconsistentDatabase);

		Instant returnValue = null;
		
		//If there is more than one version
		if (lvt.size()>1) {
			//Trash object and update the imported timestamp int he master record
			trashObjectVersion(handle,lvt.get(0).imported,lvt.get(1).imported);
			returnValue= lvt.get(1).imported;
		} else {
			returnValue=lvt.get(0).imported;
		}
		
		return returnValue;
		
	}
	
	/**
	 * Delete an object from the repository, moving any associated content files 
	 * to the trash folder.  Objects are normally only removed if the following
	 * constraints are met:<p>
	 * 
	 * <ul>
	 *    <li> There are no tag assignments associated with the object.
	 *    <li> The object master and detail records are consistent.
	 *    <li> The meta data is consistent with the object store (files actually exist).
	 * </ul>
	 * 
	 * The {@code force} parameter can be used to override consistency errors, however
	 * the object will be obliterated from the system and not recoverable from
	 * the trash.<p>
	 * 
	 * After a successful execution of this function, the repository will be
	 * in the following state with regard to the retired object's {@code handle}.<p>
	 * <ul>
	 *    <li> All records in the {@code objects}, {@code versions}, and {@code object_tags}
	 *         tables keyed with {@code handle} will be removed.
	 *    <li> Any files in the repository backing store associated with deleted
	 *         versions will no longer be in repository space.
	 * </ul>
	 * 
	 * @param handle The handle of the object to retire.
	 * @param force If true, then brute force is used to remove the object 
	 *  and any database consistency complaints are ignored.
	 */
	public void retireObject(int handle, boolean force) 
			throws U_Exception 
	{
		// Check consistency (includes handle existence check)
		String validation = validate(handle);
		// If not consistent and not forced, throw exception
		if (validation!=null && !force)
			throw new U_Exception(ERROR.InconsistentDatabase,validation);
		// Gather a list of files to retire/delete
		List<VersionsTuple> versions = getVersions(handle);
		if (!force)
			for (VersionsTuple vt : versions) 
				if (vt.path==null || !Util.fileExists(vt.path))
					throw new U_Exception(ERROR.NoSuchFile,vt.path==null?"null":vt.path);
		// Start transaction
		   try {
			   startTransaction();
			   // Move all related tags from object_tags to trashobject_tags
			   List<Object_tagsTuple> otags = object_tagsTable.select("handle="+handle);
			   for (Object_tagsTuple ot : otags) {
				   trashObject_tagsTable.insert(ot);
				   object_tagsTable.delete(ot);
			   }
				   
			   // Move all version records from versions to trashversions
			   for (VersionsTuple vt : versions) {
				   trashVersionsTable.insert(vt);
				   versionsTable.delete(vt);
			   }
			   // Delete the objects record
			   ObjectsTuple ot = getObject(handle);
			   objectsTable.delete(ot);
			   // Commit transaction
			   commitTransaction();
			   // Move files to trash content directory
			   for (VersionsTuple vt: versions) {
				   if (vt.path!=null)
				   try {
					   File f = new File(vt.path);
					   Util.moveToTrash(f);
				   } catch (Exception e) {
					   L.log(L.W, this, "Failed to move file to trash %s", vt.path);
					   /* best effort, but we should at least log it */
				   }
			   }
		   } finally {
			   if (transactionInProgress()) {
				   try {
					   rollbackTransaction();
				   } catch (Exception e) {
					   /* best effort */
				   }
			   }
		   }
	}
	
	/**
	 * Validates the relationship of master and detail records for an object to 
	 * determine if it is consistent. A relationship is consistent if:<p>
	 * 
	 * <ol>
	 *  <li> There is at least one detail (version) record.
	 *  <li> The imported date of the master record matches the most recent 
	 *  imported date in the detail records.
	 *  <li> The version counts in the detail records are ascending (1, 2, 4, 5, 9...)
	 * </ol>
	 *  
	 *  @param handle The handle of the object to validate.
	 *  @return Null if the relationship is valid, otherwise a string indicating 
	 *  the problem. 
	 */
	public String validate(int handle) throws U_Exception {
		ObjectsTuple ot = getObject(handle);
		List<VersionsTuple> lvt = getVersions(handle);
		if (lvt.size()==0)
			return "There are no version records.";
		if (!lvt.get(0).imported.equals(ot.imported)) 
			return "Master record and detail records disagree on most recently imported.";
		for (int x=1; x>lvt.size(); x++)
			if (lvt.get(x).versioncount < lvt.get(x-1).versioncount)
				return "Version counts of detail records are not ordered.";
		return null;
	}
	
	/**
	 * Returns a printable report string about the overall health of an object.
	 * 
	 * @param handle The handle of the object in question
	 * @return A string containing the report.
	 * @throws U_Exception
	 */
	public String status(int handle) 
			throws U_Exception 
	{
		StringBuffer sb = new StringBuffer();
		sb.append("OBJECT HANDLE: "+handle+"\n");
		sb.append("+---------------------------------+\n");
		sb.append("| Database Consistency Validation |\n");
		sb.append("+---------------------------------+\n\n");
		String v = validate(handle);
		sb.append(v==null?"The object is consistent":"The object is NOT consistent because: "+v);
		sb.append("\n\n");
		sb.append("+---------------------------------+\n");
		sb.append("|   Content Health Check Report   |\n");
		sb.append("+---------------------------------+\n\n");

		v = healthMonitoringReport(handle);
		sb.append(v==null?"A report was not returned!":v);
		return sb.toString();
	}
	
	/**
	 * Performs a content health check for the most recent version of the content object.  
	 * This is concerned with the integrity of the
	 * bits comprising the object and not the consistency of the database metadata, although some
	 * ancillary metadata, such as linkcount and inode, are computed and updated as part of the check.
	 * 
	 * @param v_handle The handle of the object to check.
	 * @param fullValidation If true, computes a new checksum and compares to the recorded (known good) checksum.
	 * @return True if the object content is deemed healthy.
	 */
	public boolean check(int v_handle, boolean fullValidation) 
		throws U_Exception 
	{
		ObjectsTuple ot = getObject(v_handle);
		VersionsTuple vt = getLatestVersion(v_handle);
		if (!ot.imported.equals(vt.imported))
			throw new U_Exception(ERROR.InconsistentDatabase);

		/* these are the newer observed values for the health monitoring data */
		/* Initially, we consider the worst case scenario, and update values as we perform the validation */
		boolean n_hm_missing = true;
		boolean n_hm_unhealthy = true;
		boolean n_hm_lengthmismatch = true;
		boolean n_hm_corrupt = true;
		Instant n_hm_lastvalidationattempt = Instant.now();
		Instant n_hm_lastsuccessfulvalidation=vt.hm_lastsuccessfulvalidation;
		Instant n_hm_lastseen = vt.hm_lastseen;
		Instant n_hm_lastfingerprinted = null;
		long n_hm_linkcount=0;
		long n_inode=0;
		String n_hm_message = "Validation did not complete - reason unknown";
		String n_sha1sum="(notcomputed)";
		boolean n_hm_healthchanged = false;
		
		
		try { //--------------- this encloses the validation workflow
			/* Does the path exist? */
			if (vt.path==null)
				throw new U_Exception(U_Exception.ERROR.ValidationError,
					String.format("Validation failed for %d: path is null",vt.handle));
			File f = new File(vt.path);
			if (!f.exists())
				throw new U_Exception(U_Exception.ERROR.ValidationError,
					String.format("Validation failed for %d: path not found",vt.handle));
			n_hm_missing=false;
			n_hm_lastseen = n_hm_lastvalidationattempt;
			//Link count should be updated here
			
			/* Does the length match? */
			if (f.length()!=vt.length) 
				throw new U_Exception(U_Exception.ERROR.ValidationError,
					String.format("Validation failed for %d: length mismatch (correct value=%d observed=%d",
							vt.handle,vt.length,f.length()));
			n_hm_lengthmismatch = false;
			
			/* if we're doing full validation, we need to compute the checksum */
			if (fullValidation) {
				try {
					n_sha1sum=Util.computeChecksum(f.getAbsolutePath());
				} catch (Exception e) {
					throw new U_Exception(U_Exception.ERROR.ValidationError,
						String.format("Validation failed for %d: error during computeChecksum: %s",
								vt.handle,e.getMessage()));
				}
				if (!n_sha1sum.equals(vt.sha1sum))
					throw new U_Exception(U_Exception.ERROR.ValidationError,
							String.format("Validation failed for %d: checksum mismatch (correct value=%s observed=%s",
									vt.handle,vt.sha1sum,n_sha1sum));
				n_hm_corrupt=false;
				n_hm_lastfingerprinted=n_hm_lastvalidationattempt;
			}
			
			/* Check consistency of the i-node */
			try {
				Util.StatBuf statBuf = Util.stat(vt.path);
				n_hm_linkcount = statBuf.linkcount;
				n_inode = statBuf.inode;
			} catch (Exception e) {
				throw new U_Exception(U_Exception.ERROR.ValidationError,
					String.format("Validation failed for %d: stat of file failed: %s",vt.handle,e.getMessage()));
			}
			
			n_hm_unhealthy=false;
			n_hm_lastsuccessfulvalidation=n_hm_lastvalidationattempt;
			
			n_hm_message = "Validation successful";
			
		} catch (U_Exception ve) {  //--this is where we log the problem
			n_hm_message = ve.getMessage();
		} finally {  //-- we will update the versions record based on the last values recorded in the workflow
			
			StringBuffer hm_lastobservedchanges = new StringBuffer();
			
			if (!vt.hm_lastseen.equals(n_hm_lastseen)) {
				//n_hm_healthchanged=true;
				//hm_lastobservedchanges.append("hm_lastseen="+n_hm_lastseen+"");
				vt.hm_lastseen=(n_hm_lastseen);
			}
			
			if (vt.hm_missing!=n_hm_missing) {
				n_hm_healthchanged=true;
				hm_lastobservedchanges.append("hm_missing="+n_hm_missing+"; ");
				vt.hm_missing=(n_hm_missing);
			}
			
			if (vt.hm_unhealthy!=n_hm_unhealthy) {
				n_hm_healthchanged=true;
				hm_lastobservedchanges.append("hm_unhealthy="+n_hm_unhealthy+"; ");
				vt.hm_unhealthy=(n_hm_unhealthy);
			}
			
			if (vt.hm_lengthmismatch!=n_hm_lengthmismatch) {
				n_hm_healthchanged=true;
				vt.hm_lengthmismatch=(n_hm_lengthmismatch);
				hm_lastobservedchanges.append("hm_lengthmismatch="+n_hm_lengthmismatch+"; ");
			}

			if (vt.hm_linkcount!=n_hm_linkcount) {
				n_hm_healthchanged = true;
				hm_lastobservedchanges.append("hm_linkcount changed from "+vt.hm_linkcount+" to "+n_hm_linkcount+"; ");
				vt.hm_linkcount=(n_hm_linkcount);
			}
			
			if (vt.inode!=n_inode) {
				n_hm_healthchanged = true;
				hm_lastobservedchanges.append("inode changed from "+vt.inode+" to "+n_inode+"; ");
				vt.inode=(n_inode);
			}
			
			if (!vt.hm_lastsuccessfulvalidation.equals(n_hm_lastsuccessfulvalidation)) {
				//n_hm_healthchanged=true;
				//hm_lastobservedchanges.append("hm_lastsuccessfulvalidation="+n_hm_lastsuccessfulvalidation+"\n");
				vt.hm_lastsuccessfulvalidation=(n_hm_lastsuccessfulvalidation);				
			}
			
			vt.hm_lastvalidationattempt=(n_hm_lastvalidationattempt);
			
			if (fullValidation) {
				if (vt.hm_corrupt!=n_hm_corrupt) {
					n_hm_healthchanged=true;
					hm_lastobservedchanges.append("hm_corrupt="+n_hm_corrupt+"\n");
					vt.hm_corrupt=(n_hm_corrupt);
				} else {
					vt.hm_lastfingerprinted=(n_hm_lastfingerprinted);
				}
			}

			vt.hm_message=(n_hm_message);
			vt.hm_healthchanged=(n_hm_healthchanged);
			vt.hm_lastobservedchanges=(hm_lastobservedchanges.toString());
			vt.hm_missing=(n_hm_missing);
			vt.hm_unhealthy=(n_hm_unhealthy);
			vt.hm_lengthmismatch=(n_hm_lengthmismatch);
			vt.hm_corrupt=(n_hm_corrupt);
			vt.hm_lastvalidationattempt=(n_hm_lastvalidationattempt);
			vt.hm_lastsuccessfulvalidation=(n_hm_lastsuccessfulvalidation);
			vt.hm_lastseen=(n_hm_lastseen);
			vt.hm_lastfingerprinted=(n_hm_lastfingerprinted);
			vt.hm_linkcount=(n_hm_linkcount);
			vt.inode=(n_inode);
			vt.hm_message=(n_hm_message);
			vt.sha1sum=(n_sha1sum);
			vt.hm_healthchanged=(n_hm_healthchanged);
			
			versionsTable.update(vt);
		}
		return !n_hm_unhealthy;
	}

	/**
	 * Returns a string that is a report of the object's most recent version health assessment, as determined on
	 * the last check() of it.
	 * 
	 * @param v_handle The handle fo the content object to report on
	 * @return A string with embedded linefeeds that contains the report content
	 */
	public String healthMonitoringReport(int v_handle) 
			throws U_Exception 
	{

		VersionsTuple vt = getLatestVersion(v_handle);
	
		StringBuffer sb = new StringBuffer();
		
		sb.append("OBJECT STATUS REPORT\n");
		sb.append("--------------------\n\n");
		sb.append("Object handle...........: "+vt.handle+"\n");
		sb.append("Imported at.............: "+vt.imported+"\n");
		sb.append("Version count...........: "+vt.versioncount+"\n");
		sb.append("Validated SHA1 checksum.: "+vt.sha1sum+"\n");
		sb.append("Validated length (bytes): "+vt.length+"\n");
		sb.append("Version title...........: "+vt.title+"\n");
		sb.append("Version path............: "+vt.path+"\n");
		sb.append("Version copied from.....: "+vt.copiedfrom+"\n");
		sb.append("Version unique file id..: "+vt.inode+"\n");
		sb.append("Hard link count.........: "+vt.hm_linkcount+"\n");
		sb.append("\n\n----Health Monitoring Summary----\n\n");
		
		if (vt.hm_lastsuccessfulvalidation==null)
			sb.append("The object has never been successfully validated.\n");
		else
			sb.append("This object was last verified healthy at "+vt.hm_lastsuccessfulvalidation+"\n");
		
		if (vt.hm_lastvalidationattempt==null) 
			sb.append("Validation has never been attempted.\n");
		else {
			if (vt.hm_lastsuccessfulvalidation!=null && !vt.hm_lastsuccessfulvalidation.toString().equals(vt.hm_lastvalidationattempt.toString())) 
				sb.append("The last attempt at validation at "+vt.hm_lastvalidationattempt+" failed or discovered a health problem.\n");
			sb.append("The last validation attempt reported this message:\n");
			sb.append("  "+vt.hm_message+"\n");
		}
		
		if (vt.hm_lastsuccessfulvalidation!=null) {
			if (vt.hm_healthchanged) {
				sb.append("A change in health status was noted on the last validation attempt.\n");
				sb.append("   The changes observed were: "+vt.hm_lastobservedchanges+"\n\n");
			}
			
			if (vt.hm_unhealthy) {
				sb.append("\nThe object is **UNHEALTHY** for these reasons:\n");
				int count=0;
				if (vt.hm_missing){
					count++;
					sb.append("-- It is missing: there is no file at the designated path.\n");
					if (vt.hm_lastseen==null)
						sb.append("   There is no record of ever having seen this file on disk.\n\n");
					else
						sb.append("   A file was last seen in this location at "+vt.hm_lastseen+"\n\n");
				} else if (vt.hm_lengthmismatch) {
					count++;
					sb.append("-- Its length has changed and no longer matches the originally observed length.\n");
				} else	if (vt.hm_corrupt){
					count++;
					sb.append("-- It is corrupt: the checksum no longer matches validated checksum.\n");
					if (vt.hm_lastfingerprinted==null)
						sb.append("   There is no record of ever having performed a checksum on this file.\n\n");
					else
						sb.append("   The file last passed checksum validation at "+vt.hm_lastfingerprinted+"\n\n");
				}
				
				if (count==0)
					sb.append("  ?. The reason is not known.\n");
			} else {
				sb.append("The object is healthy.\n");
			}
		}
		
		sb.append("\n\n----Health Monitoring Details----\n\n");
		sb.append("hm_unhealthy=").append(vt.hm_unhealthy).append('\n');
		sb.append("hm_missing=").append(vt.hm_missing).append('\n');
		sb.append("hm_lengthmismatch=").append(vt.hm_lengthmismatch).append('\n');
		sb.append("hm_corrupt=").append(vt.hm_corrupt).append('\n');
		sb.append("hm_lastseen=").append(Util.nullWrap(vt.hm_lastseen)).append('\n');
		sb.append("hm_lastfingerprinted=").append(Util.nullWrap(vt.hm_lastfingerprinted)).append('\n');
		sb.append("hm_lastvalidationattempt=").append(Util.nullWrap(vt.hm_lastvalidationattempt)).append('\n');
		sb.append("hm_lastsuccessfulvalidation=").append(Util.nullWrap(vt.hm_lastsuccessfulvalidation)).append('\n');
		sb.append("hm_healthchange=").append(vt.hm_healthchanged).append('\n');
		sb.append("hm_message=").append(vt.hm_message).append('\n');		
		sb.append("hm_lastobservedchanges=").append(vt.hm_lastobservedchanges).append('\n');

		return sb.toString();
	}
	
	/**
	 * Determines if there is an object in the database with the specified handle.
	 * @param handle
	 * @return
	 */
	public boolean objectExists(int handle)
		throws U_Exception
	{
		return getObject(handle)!=null;
	}
	
	/**
	 * Private routine used to vet handle values as representing existant objects.
	 * @param handle
	 * @throws U_Exception
	 */
	private void vetHandle(int handle) throws U_Exception {
		if (!objectExists(handle))
			throw new U_Exception(ERROR.NoSuchHandle,Integer.toString(handle));
	}
//------------------------------------------------------------------------------------------	
//-- TAG DEFINITIONS
//------------------------------------------------------------------------------------------

	/**
	 * Creates a new tag.
	 * @param name The name of the tag. 
	 * @param type The type (Entity, Sequence, Category). 
	 * @param description The optional description.
	 * @param browsing_priority The optional integer value that gives a hint to 
	 *   repository browsers about the usefulness of this tag for browsing activities.
	 *   Use {@code null} to indicate no priority is provided.
	 * @throws U_Exception
	 */
	public void createTag(
			String name, 
			String type, 
			String description, 
			Integer browsing_priority) 
		throws U_Exception
	{
		checkNull(name);
		checkNull(type);
		TagsTuple tt = new TagsTuple();
		tt.name=(name);
		tt.type=(type);
		tt.description=(description);
		tt.browsing_priority=(browsing_priority);
		tagsTable.insertIfNew(tt);
	}

	/**
	 * Determines if a tag exist (is defined).
	 * @param name The name of the tag to check.
	 * @return True if the tag name is defied.
	 * @throws U_Exception
	 */
	public boolean tagExists(String name) throws U_Exception {
		return getTag(name)!=null;
	}
	
	/**
	 * Private method that vets tag names.
	 * @param name
	 * @throws U_Exception
	 */
	private void vetTagName(String name) throws U_Exception {
		if (name==null || !tagExists(name))
			throw new U_Exception(ERROR.NoSuchTagName,Util.nullWrapShow(name));
	}
	
	/**
	 * Returns a set of all tag records
	 * @return List of {@code TagsTuple}
	 * @throws U_Exception
	 */
	public List<TagsTuple> getTags() throws U_Exception
	{
		return tagsTable.select();
	}	

	/**
	 * Returns the tag tuple associated with name, or null if it does not exist.
	 * @param name The name of the tag to get.
	 * @return The tag tuple for the specified tag, or null if it does not exist.
	 */
	public TagsTuple getTag(String name) throws U_Exception {
		try {
			List<TagsTuple> ltt = 
					tagsTable.select(tagsTable.EF.comp("@name","=",name), SortDirective.NONE);
			if (ltt.size()>0) 
				return ltt.get(0);
			return null;
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.ExpressionError,e);
		}
	}
	
	/**
	 * Deletes a tag.  No check is made to determine if the tag name actually exists.
	 * @param name The name of the tag to be deleted.
	 * @throws U_Exception
	 */
	public void deleteTag(String name) throws U_Exception
	{
		vetTagName(name);
		if (tagIsInUse(name))
			throw new U_Exception(ERROR.ConstraintViolation,"Tag is in use: "+name);
		try (TransactionManager tm = new TransactionManager()) {
			for (Tag_valuesTuple tvt : getTagValues(name))
				tag_valuesTable.delete(tvt);
			TagsTuple tt = new TagsTuple();
			tt.name=name;
			tagsTable.delete(tt);
			tm.commit();
			return;
		} catch (U_Exception e) {
			throw new U_Exception(ERROR.Unexpected,"Unexpected database error during tag name deletion",e);
		}
	}

//------------------------------------------------------------------------------------------	
//-- TAG VALUE DEFINITIONS
//------------------------------------------------------------------------------------------

	/**
	 * Creates a name=value pair if it doesn't already exist.  Throws an exception if
	 * the tag name isn't defined.
	 * @param name 
	 * @param value
	 * @throws U_Exception
	 */
	public void createTagValue(String name, String value)
		throws U_Exception 
	{
		vetTagName(name);
		try {
			Tag_valuesTuple tvt = new Tag_valuesTuple();
			tvt.name=(name);
			tvt.value=(value);
			tag_valuesTable.insertIfNew(tvt);
		} catch (Exception e) { 
			throw new U_Exception(ERROR.Unexpected,"An unexpected database error occured during name=value creation",e);
		}
	}	

	/**
	 * Determines if a tag/value pair has been defined.
	 * @param name The name of the pair.
	 * @param value The value of the pair.
	 * @return True if the tag/value pair has been defined.
	 * @throws U_Exception
	 */
	public boolean tagValueExists(String name, String value) throws U_Exception {
		try {
			TupleExpressionFactory ef = tag_valuesTable.EF;
			return tag_valuesTable.select(ef.and(ef.comp("@name","=",name),ef.comp("@value","=",value)),SortDirective.NONE).size()>0;
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.ExpressionError,e);
		}
	}
	
	/**
	 * Private routine used to vet tag name/values.
	 * @param name
	 * @param value
	 * @throws U_Exception
	 */
	private void vetTagValue(String name, String value) throws U_Exception {
		if (name==null || value==null || !tagValueExists(name,value))
			throw new U_Exception(ERROR.NoSuchTagValuePair,
					String.format("%s=%s",Util.nullWrapShow(name), Util.nullWrapShow(value)));
	}
	
	/**
	 * Determines if a tag name has any values associated with it.
	 * @param name The tag in question.
	 * @return True if there are values defined for this tag.
	 * @throws U_Exception
	 */
	public boolean tagHasDefinedValues(String name) throws U_Exception {
		try {
			return tag_valuesTable.select(tagsTable.EF.comp("@name", "=", name),1).size()>0;
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
	}

	/**
	 * Returns a list of all tag/value records
	 * @return
	 * @throws U_Exception
	 */
	public List<Tag_valuesTuple> getTagValues() throws U_Exception {
		return tag_valuesTable.select();
	}

	/**
	 * Returns a list of all tag/values for the given tag name. Throws an exception if the
	 * tag name is not defined.
	 * @param name
	 * @return
	 * @throws U_Exception
	 */
	
	public List<Tag_valuesTuple> getTagValues(String name) throws U_Exception {
		try {
			vetTagName(name);
			return tag_valuesTable.select(tag_valuesTable.EF.comp("@name", "=", name),SortDirective.build("value",SortDirective.Order.Ascending));
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
	}

	/**
	 * Undefines a name/value pair.  Throws an exception if the name/value pair is not defined
	 * or is in use.
	 * @param name
	 * @param value
	 * @throws U_Exception
	 */
	public void deleteTagValue(String name, String value) 
		throws U_Exception
	{
		if (tagValueInUse(name, value))
			throw new U_Exception(ERROR.ConstraintViolation,String.format("Tag value is in use ('%s'='%s')",name,value));
		Tag_valuesTuple tvt = new Tag_valuesTuple();
		tvt.name=(name);
		tvt.value=(value);
		try {
			tag_valuesTable.delete(tvt);
		} catch (U_Exception e) {
			throw new U_Exception(ERROR.Unexpected,"Unexpected database error during tag value deltion",e);
		}
	}

	
//------------------------------------------------------------------------------------------	
//-- OBJECT TAGGING
//------------------------------------------------------------------------------------------
	
	/**
	 * Tags an object with the given name and value, provided the tag/value has been defined.
	 * 
	 * @param handle
	 * @param name
	 * @param value
	 * @throws U_Exception
	 */
	public void tagObject(int handle, String name, String value) 
		throws U_Exception 
	{
		vetHandle(handle);
		vetTagValue(name,value);
		Object_tagsTuple vtt = new Object_tagsTuple();
		vtt.name=(name);
		vtt.value=(value);
		vtt.handle=(handle);
		object_tagsTable.insertIfNew(vtt);
	}

	/**
	 *  Returns a list of tag/value records for a specific object.  Throws an exception
	 *  if the object handle is not assigned.
	 * @param handle The object of interest.
	 * @return A list of Video_tagsTuple objects associated with the specified
	 * handle value.
	 * @throws U_Exception
	 */
	public List<Object_tagsTuple> getTagValuesForObject(int handle) throws U_Exception {

		try {
			vetHandle(handle);
			return object_tagsTable.select(
					object_tagsTable.EF.comp("@handle", "=", handle), 
					SortDirective.build("name",Order.Ascending,"value",Order.Ascending)
					);
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,e);
		}
	}
	
	/**
	 * Returns a list of tag/value records for a specific object and tag name.
	 * Throws an exception if the object or tag name do not exist.
	 * @param handle The object of interest.
	 * @param tagname The tag name of interest.
	 * @return A list of Video_tagsTuple objects associated with the specified
	 * handle value.
	 * @throws U_Exception
	 */
	public List<Object_tagsTuple> getTagValuesForObject(int handle, String tagname) throws U_Exception {

		try {
			vetHandle(handle);
			vetTagName(tagname);
			return object_tagsTable.select(
				object_tagsTable.EF.and(
					object_tagsTable.EF.comp("@handle", "=", handle),
					object_tagsTable.EF.comp("@name", "=", tagname)
					)
				);
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,e);
		}
	}
	
	/**
	 * Returns a sorted collection of handles that are tagged with a specific tagname
	 * and any value. Throws an exception if the tagname is not defined.
	 * @param tagname The tagname of interest.
	 * @return A List<Integer> of handles tagged with tagname and any value.
	 * @throws U_Exception
	 */
	public SortedSet<Integer> getObjectsTaggedWith(String tagname) throws U_Exception {
		try {
			vetTagName(tagname);
			SortedSet<Integer> handles = new TreeSet<Integer>();
			for (Tuple t : object_tagsTable.select(object_tagsTable.EF.comp("@name", "=", tagname))) {
				handles.add(((Object_tagsTuple)t).handle);
			}
			return handles;
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,e);
		}
	}

	/**
	 * Returns a collection of handles that are tagged with a specific tagname
	 * and tag value.  Throws an exception if the name/value pair is not defined.
	 * @param tagname The tagname of interest.
	 * @param tagvalue The tagvalue of interest.
	 * @return A List<Integer> of handles tagged with tagname=tagvalue.
	 * @throws U_Exception
	 */
	public SortedSet<Integer> getObjectsTaggedWith(String tagname, String tagvalue) throws U_Exception {
		try {
			vetTagValue(tagname,tagvalue);
			SortedSet<Integer> handles = new TreeSet<Integer>();
			for (Tuple t : object_tagsTable.select(
					object_tagsTable.EF.and(
							object_tagsTable.EF.comp("@name", "=", tagname),
							object_tagsTable.EF.comp("@value", "=", tagvalue)
					)
				 )	
				) {
				handles.add(((Object_tagsTuple)t).handle);
			}
			return handles;
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.Unexpected,e);
		}
	}

	/**
	 * Determines if an object has been tagged a particular way.  Throws an exception
	 * if the object, name=value pair does not exist.
	 * @param handle  The handle of the object to test.
	 * @param name The name of the tag in question.
	 * @param value The value of the tag in question.
	 * @return True if the object is tagged as specified.
	 * @throws U_Exception
	 */
	public boolean objectIsTagged(int handle, String name, String value) throws U_Exception{
		try {
			vetHandle(handle);
			vetTagValue(name,value);
			TupleExpressionFactory ef = object_tagsTable.EF;
			return object_tagsTable.select(
					ef.and(
						ef.comp("@handle","=",handle),
						ef.comp("@name","=",name),
						ef.comp("@value","=",value)
					),
					SortDirective.NONE
				).size()>0;
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.ExpressionError,e);
		}
	}

	/**
	 * Determines if a tag name is currently in use (at least one object is tagged with it).
	 * Throws an exception if the tag name is not defined.
	 * @param name The tag in question.
	 * @return True if at least one object is tagged with this tag.
	 * @throws U_Exception
	 */
	public boolean tagIsInUse(String name) throws U_Exception {
		try {
			vetTagName(name);
			return object_tagsTable.select(object_tagsTable.EF.comp("@name","=",name),1).size()>0;
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
	}
		
	/**
	 * Determines if a tag/value pair is in use, meaning at least one object is tagged 
	 * with this combination.  Throws an exception if the name/value pair is not defined.
	 * @param name The tag in question.
	 * @param value The tag value in question.
	 * @throws U_Exception
	 */
	public boolean tagValueInUse(String name, String value) throws U_Exception {
		try {
			vetTagValue(name,value);
			return object_tagsTable.select(
					tag_valuesTable.EF.and(
							tag_valuesTable.EF.comp("@name", "=", name),
							tag_valuesTable.EF.comp("@value", "=", value)
							),1).size()>0;
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
	}
	
	/**
	 * Removes a tag on an object. Throws an exception if the object or the name=value pair
	 * does not exist.
	 * @param handle  The handle of the object to test.
	 * @param name The name of the tag in question.
	 * @param value The value of the tag in question.
	 * @throws U_Exception If an error occurs.
	 */
	public void untagObject(int handle, String name, String value)
		throws U_Exception
	{
		vetHandle(handle);
		vetTagValue(name,value);
		Object_tagsTuple vtt = new Object_tagsTuple();
		vtt.name=(name);
		vtt.value=(value);
		vtt.handle=(handle);
		try {
			object_tagsTable.delete(vtt);
		} catch (U_Exception e) { /* ignore */ }

	}
	
//------------------------------------------------------------------------------------------	
//-- Common queries
//------------------------------------------------------------------------------------------

	/**
	 * Get the list of attribute names a client can use through
	 * the query language interface.
	 * @return An array of the attribute names.
	 */
	public String[] getClientQueryParameterList() {
		return versionsTable.attributesArray();
	}
	
	/**
	 * Checks to see if an object exists in the database with the same fingerprint, which is
	 * both the checksum and length consider together.
	 * 
	 * @param sha1sum The checksum of the file in question.
	 * @param length The length of the file in question.
	 * @return  The handle of the colliding object, or 0 if there is no collision.
	 * @throws U_Exception
	 */
	
	public int fingerprintExists(String sha1sum, long length) throws U_Exception {
		try {
			TupleExpressionFactory ef = versionsTable.EF;
			List<VersionsTuple> l = versionsTable.select(
					ef.and(ef.comp("@sha1sum", "=", sha1sum),ef.comp("@length", "=", length)),
					SortDirective.NONE
					);
			if (l.size()==0) return 0;
			return l.get(0).handle;
			
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.ParserError,e);
		}
	}
	
	/**
	 * Returns the versions tuple for the specified import timestamp of handle. 
	 * Throws an exception if the handle is not assigned (object does not exist).
	 * 
	 * @param handle The handle of the object sought.
	 * @return The tuple, or null if there is no such object.
	 * @throws U_Exception
	 */
	public VersionsTuple getVersion(int handle, Instant imported) throws U_Exception {
		if (imported==null)
			throw new U_Exception(U_Exception.ERROR.BadParameter,"imported");
		vetHandle(handle);
		try {
			TupleExpressionFactory ef = versionsTable.EF;
			
			List<VersionsTuple> l = versionsTable.select(
					ef.and(ef.comp("@handle", "=", handle),ef.comp("@imported","=",imported)),
					SortDirective.NONE
					);
			if (l.size()==0) 
				throw new U_Exception(ERROR.InconsistentDatabase,"No versions available for existant object "+Integer.toString(handle));
			return l.get(0);
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.ExpressionError,e);			
		}
	}
	
	/**
	 * Returns the versions tuple for the latest version of handle. Throws an exception if
	 * handle is not assigned (object does not exist).
	 * 
	 * @param handle The handle of the object sought.
	 * @return The tuple, or null if there is no such object.
	 * @throws U_Exception
	 */
	public VersionsTuple getLatestVersion(int handle) throws U_Exception {
		vetHandle(handle);
			
		List<VersionsTuple> l = getVersions(handle);
		if (l.size()==0) return null;
		return l.get(0);
	}
	
	
	public List<VersionsTuple> getLatestVersions() throws U_Exception {
		return currentVersionsTable.select();
	}
		
	/**
	 * Returns the ObjectsTuple for the specified handle, or null if non existent.
	 * @param handle
	 * @return
	 * @throws U_Exception
	 */
	public ObjectsTuple getObject(int handle) throws U_Exception {
		try {
			
			List<ObjectsTuple> l = objectsTable.select(objectsTable.EF.comp("@handle", "=", handle),SortDirective.NONE);
			if (l.size()==0) return null;
			return l.get(0);
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.ExpressionError,e);
		}
	}
	
		/**
	 * Does the heavy lifting for creating the database entries of a new version, including the
	 * case where this is a new object, and the case where it is a pre-existing object.
	 * 
	 * @param vt The source of values to be used in populating the new version.  The following attributes must be
	 *            set prior to passing <code>vt</code> to this method.
	 *           <ol>
	 *             <li><code>handle</code> - the handle of the object for which to create a new version, 
	 *                                       or <code>0</code> to create a new object</li>
	 *             <li><code>path</code> - the path to the file being imported as the new version's content</li>
	 *             <li><code>sha1sum</code> - the pre-computed checksum of the file referenced by <code>cv.path</code></li>
	 *             <li><code>length</code> - the length of the file referenced by <code>cv.path</code></li>
	 *             <li><code>title</code> - the title for this version.  If <code>null</code> and this is
	 *                                      the first version (new object), then derive title from the pathname, otherwise
	 *                                      keep the title from the previous version</li>
	 *             <li><code>copiedfrom</code> - the file from which content was copied to create the contents of <code>path</code></li>
	 *             <li><code>inode</code> - the *nix inode number to which <code>path</code> refers</li>
	 *           </ol>
	 * @param preCommitLambda A lambda expression of the form   <code>() -> {code}</code> that must return a boolean.
	 *                        This function is executed after all database
	 *                        operations in the open transaction have been submitted, but not committed.  If this code block
	 *                        returns <code>true</code>, then the transaction will be committed. If this code block
	 *                        returns <code>false</code>, then the transaction will be rolled back and an Exception thrown.
	 * @throws U_Exception
	 */
	public void addVersion(VersionsTuple vt, LambdaNoParmsU preCommitLambda)
		throws U_Exception
	{
		connection.startTransaction();
		try /* protect transaction with finally */ {
			
			//Regardless of whether old or new, the import time will be NOW
			//And all the health maintenance values will be the defaults.
			Instant now = Instant.now();
			vt.imported=(now);
			vt.hm_lastfingerprinted=(now); 
			vt.hm_lastseen=(now);
			vt.hm_lastsuccessfulvalidation=(now);
			vt.hm_lastvalidationattempt=(now);
			
			vt.hm_corrupt=(false);
			vt.hm_healthchanged=(false);
			vt.hm_lengthmismatch=(false);
			vt.hm_unhealthy=(false);
			
			//If we have been given no object handle, create a a new objects tuple and get a handle for it
			if (vt.handle==0) {
				ObjectsTuple ot = new ObjectsTuple();
				ot.imported=(vt.imported);
				if (vt.title==null || vt.title.equals(""))
					vt.title=(Util.deriveTitle(vt.path));
				objectsTable.insert(ot);
				vt.handle=(ot.handle);
				versionsTable.insert(vt);
			} 
			
			//Else it exists and we need to increment/create a new version
			else /* handle!=0 */ {
				
				//Does an object with that handle exist?
				ObjectsTuple ot = getObject(vt.handle);
				if (ot==null)
					throw new U_Exception(U_Exception.ERROR.NoSuchHandle,String.valueOf(vt.handle));
				
				//Does the latest version of the object agree?
				VersionsTuple lt = getLatestVersion(vt.handle);
				if (!lt.imported.equals(ot.imported))
					throw new U_Exception(U_Exception.ERROR.InconsistentDatabase,"Object/version records for handle "+vt.handle+" are not in a consistent state and cannot be manipulated until repaired");

				//The only things we need to retain from the old version to the new are:
				//   The versioncount (incremented)
				//   The title (if we were given no title)
				vt.versioncount=(lt.versioncount+1);
				if (vt.title==null || vt.title.equals(""))
					vt.title=(lt.title);
				versionsTable.insert(vt);
				
				ot.imported=(vt.imported);
				objectsTable.update(ot);
			}
			
			//Call additional code prior to commit  
			//It will throw a U_Exception if we should not commit
			preCommitLambda.op();
			
			connection.commitTransaction();
		} finally {
			if (connection.transactionInProgress())
				connection.rollbackTransaction();
		}
	}

	/**
	 * Removes a version record from the repository, transferring the version record to the {@code trashversions} table,
	 * and moving the content file to the trash directory.  The path of the version record in {@code trashversions} is
	 * updated to reflect the file's new location in the trash. This path update is not guaranteed to occur, but is highly
	 * unlikely to fail.
	 * 
	 * This routine does not impose consistency checks on the database.  It doesn't make sure it is deleting an older or 
	 * current version, for example. It is just a workhorse routine for other routines that perform rollback, retirement,
	 * and deletion of objects and object versions in the repository.
	 * 
	 * @param handle The handle of the object for which an older version is being trashed
	 * @param imported The timestamp of the object being trashed
	 * @param updatedVideoImported If not null, then the object record will be updated with this value.
	 * 
	 */
	public void trashObjectVersion(int handle, Instant imported, Instant updateVideoImported) 
			throws U_Exception 
	{
		
		/*
		 * 1. Retrieve the version record for handle/imported from the versions table
		 * 2. Begin transaction
		 * 3.    Copy the version record to trashversions
		 * 4.    Delete the version record from versions
		 * 5.    Move the file to its trash location
		 * 6. Commit the transaction
		 * 7. Atempt to update the path value for the trashed record (best effort)
		 */


		VersionsTuple vt = getVersion(handle,imported);
		connection.startTransaction();
		try { //-- transaction wrapper
			
			trashVersionsTable.insert(vt);
			versionsTable.delete(vt);
				
			//Move current path to the trash
			String trashName = null;

			trashName = Util.moveToTrash(new File(vt.path));
			
			if (updateVideoImported!=null) {
				ObjectsTuple ot = new ObjectsTuple();
				ot.handle=(handle);
				ot.imported=(updateVideoImported);
				objectsTable.update(ot);
			}
				
			connection.commitTransaction();
			
			vt.path=(trashName);
			try {
				trashVersionsTable.update(vt);
			} catch (U_Exception e) {
				 L.log(L.I,this,"Unable to update path of trashed object '%s'",trashName);
			}
		} finally {
			if (connection.transactionInProgress())
				connection.rollbackTransaction();
		}

	}
	
	/**
	 * Returns a list of version tuples for the specified handle, sorted in descending order by imported date.
	 * @param handle The handle of the object of interest.
	 * @return A list of version tuples for the object.
	 */
	
	public List<VersionsTuple> getVersions(int handle) throws U_Exception {
		try {
			vetHandle(handle);
			return versionsTable.select(versionsTable.EF.comp("@handle", "=", handle),SortDirective.build("imported",SortDirective.Order.Descending));
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.ExpressionError,e);
		}
		
		
		
	}

	//------ tag membership testing cache -------------------------------------------------
	//
	/*
	 * Determination of tag set membership (is an integer handle i in the set name:value)
	 * can be very expensive when querying underlying table sources.  The tag membership
	 * testing cache loads tagging data in set relationship form into memory. When the cache
	 * is avaiable, methods that perform tag membership tests will use it. Over time, the
	 * cache may be come stale and must be refreshed.  There are settings (variables) that
	 * can enable auto-refresh when stale, auto-turn off when stale, and set the expiration
	 * time, used to determine when the cache as gone stale.
	 * 
	 * The tm_* testing methods do NOT check that the cache is loaded or fresh.  If they are
	 * called directly, then it is up to the caller to ensure the cache is in an appropriate
	 * state by calling tm_refreshCache().
	 */
	
	private final int CI=1;  //Case insensitive
	private final int CS=0;  //Case sensitive
	public TreeSet<Integer>tm_all_objects = null;
	
	public HashMap<String,HashMap<String,TreeSet<Integer>>[]> tm_name_value_objects= null;
	//              tag            value     handles       0=the CS map, 1=the CI map
	
	public HashMap<Integer,HashMap<String,TreeSet<String>[]>> tm_values_by_handle= null;
	//              handle          tag        values     0=the CS map, 1=the CI map
	
	private long tm_load_time = 0;;
	private int tm_secondsFromLoadUntilStale=15*60;  //15 minutes
	
	public boolean tm_cacheLoaded() {
		return tm_all_objects!=null;
	}
	
	public void tm_destroyCache() {
		tm_all_objects = null;
		tm_name_value_objects = null;
		tm_load_time = 0;
		tm_values_by_handle = null;
	}
	
	public boolean tm_cacheIsValid() {
		return tm_load_time + tm_secondsFromLoadUntilStale > Instant.now().getEpochSecond();
	}
	
	/**
	 * Called by an applySelection method which visits the tagging table sorted by tag name
	 * sorted by value.  Loads the membership testing cache data in the outer class private
	 * variables above.
	 * @author vlibrarian
	 *
	 */
	private class Tm_Xform_LoadMembershipData implements SelectionTransformer<Object_tagsTuple>  {
		String lastName="";   //The last tag name we saw
		String lastValue="";  //The last value we saw
		
		//The current map of tag names to (CS/CI value maps to handle sets)
		HashMap<String,TreeSet<Integer>> currentTagSet[] = null;
		//       value    handles                      0=CS, 1=CI (forced-lower case)
		
		//Both the CS and CI will share the same handle set
		TreeSet<Integer> currentValueSet = null;
		
		
		@SuppressWarnings({ "unchecked" })
		public boolean action(Object_tagsTuple tuple) {
			int handle = tuple.handle;   // Avoid multiple retrievals - they can be expensive
			String name = tuple.name;
			String value = tuple.value;
			String civalue = value.toLowerCase();

			if (!name.equals(lastName)) {
				lastValue="";
				currentTagSet = new HashMap[2];
				currentTagSet[CS] = new HashMap<String, TreeSet<Integer>>();
				currentTagSet[CI] = new HashMap<String, TreeSet<Integer>>();
				tm_name_value_objects.put(name, currentTagSet);
				lastName=name;
			}
			if (!value.equals(lastValue)) {
				currentValueSet = new TreeSet<Integer>();
				currentTagSet[CS].put(value, currentValueSet);
				currentTagSet[CI].put(civalue, currentValueSet);
				lastValue=value;
			}
			tm_all_objects.add(handle);
			currentValueSet.add(handle);
			
			TreeSet<String>[] valuesForThisHandle = new TreeSet[2];	
			
			//Have we seen this handle yet?
			HashMap<String,TreeSet<String>[]> tagsForThisHandle= tm_values_by_handle.get(handle);
			if (tagsForThisHandle==null) {
				//If not, we need to create a map of tag names to value sets

				tagsForThisHandle = new HashMap<String, TreeSet<String>[]>();
				//Put this new map into the master map, keyed by the handle
				tm_values_by_handle.put(handle,tagsForThisHandle);
				//We know for certain we'll need a new valueset for this tag
				valuesForThisHandle=new TreeSet[2];
				valuesForThisHandle[CS]=new  TreeSet<String>();
				valuesForThisHandle[CI]=new TreeSet<String>();
				//And that this value will be in it
				valuesForThisHandle[CS].add(value);
				valuesForThisHandle[CI].add(civalue);
				//And we need to put it into the map for this handle's tag values
				tagsForThisHandle.put(name,valuesForThisHandle);
			} else {
				//We've seen this handle. Have we seen this tag for this handle?
				valuesForThisHandle = tagsForThisHandle.get(name);
				//If not, we need to create a value set
				if (valuesForThisHandle==null) {
					valuesForThisHandle = new TreeSet[2];
					valuesForThisHandle[CS]=new TreeSet<String>();
					valuesForThisHandle[CI]=new TreeSet<String>();
					tagsForThisHandle.put(name, valuesForThisHandle);
				}
				//put the value into the set
				valuesForThisHandle[CS].add(value);
				valuesForThisHandle[CI].add(civalue);
			}
			return true;
		}
	}	
	
	public void tm_loadCache() throws U_Exception {
		tm_load_time = Instant.now().getEpochSecond();
		tm_all_objects = new TreeSet<Integer>();
		tm_name_value_objects = new HashMap<String, HashMap<String,TreeSet<Integer>>[]>();
		tm_values_by_handle = new HashMap<Integer, HashMap<String,TreeSet<String>[]>>();
		object_tagsTable.select(
				null,
				SortDirective.build("name",Order.Ascending,"value",Order.Ascending),
				0,
				new Tm_Xform_LoadMembershipData());
	}
	
	public void tm_freshenCache() throws U_Exception {
		if (!tm_cacheLoaded() || !tm_cacheIsValid())
			tm_loadCache();
	}
	
	
	public boolean tm_isTagged(int handle, String name, String value, boolean ci) {
		try {
			if (ci)
				return tm_name_value_objects.get(name)[CI].get(value.toLowerCase()).contains(handle);
			return tm_name_value_objects.get(name)[CS].get(value).contains(handle);
		} catch (Exception e) {
			return false;
		}
	}
	
	public TreeSet<String>[] tm_valuesOfTagForHandle(Integer handle, String name) {
		try {
			return tm_values_by_handle.get(handle).get(name);
		} catch (Exception e) {
			return null;
		}
	}

	/**
	 * <p>Performs a query against the {@code versions} table, returning tuples that match the provided filter
	 * and ordered according to the sort directive.</p>
	 * <p>{@code U_Exception} codes thrown directly from this method:</p>
	 * <ul>
	 *    <li>ExpressionError</Li>
	 * </ul>
	 *
	 * @param filter A filter expression that can be parsed by the {@code VersionsTuple} expression factory.
	 * @param orderby An order by expression of the form {@code "attribute ASCending|DESCending, ..."}.
	 * @return The resulting list of tuples.
	 * @throws U_Exception
	 */
	public List<VersionsTuple> processQuery(String filter, String orderby) throws U_Exception {
		ExpressionFactory.Expr node = null;
		try {
			node = WhereConditionParser.parseFilterExpression(filter, versionsTupleExpressionFactory);
		} catch (Exception e) {
			throw new U_Exception(U_Exception.ERROR.ExpressionError,e);
		}
		if (node.type!=ExpressionFactory.Type.Boolean)
			throw new U_Exception(ERROR.ExpressionError,"Filter expression must evaluate to type Boolean");
		
		final ExpressionFactory.Expr filterNode = node;
		final LinkedList<VersionsTuple> result = new LinkedList<>();
		
		SortDirective sort[] = SortDirective.build(orderby);
		
		SelectionTransformer<VersionsTuple> collector = (vt) -> {
			try {
				if ((Boolean)(filterNode.eval(vt).object))
					result.add(vt);
			} catch (ExprException e) {
				throw new U_Exception(ERROR.ExpressionError,"during evaluation of latest version of object "+vt.handle,e);
			}
			return true;
		};

		currentVersionsTable.applySelection(null /* the xform filters */, sort, 0, collector);
		return result;
	}
	
	/**
	 * Creates an XML document that describes the schema of the "object" to external clients.
	 * It is actually a description of the VersionsTuple.
	 * @param doc The XML document to use as context for the elements.
	 * @return An {@code Element} object that is the returned schema fragment.
	 */
	public Element getObjectSchema(Document doc) {
		Element elSchema = doc.createElement("schema");
		elSchema.setAttribute("type","object");
		for (String attributeName : versionsTable.attributesArray()) {
			Element elAttribute = doc.createElement("attribute");
			elAttribute.setAttribute("name",attributeName);
			String reportedType="unknown";
			switch (versionsTable.tupleTypeOf(attributeName).name()) {
			case "Integer":
			case "Long":
				reportedType="integer";
				break;
			case "Instant":
				reportedType="timestamp";
				break;
			case "String":
				reportedType="string";
				break;
			case "Boolean":
				reportedType="boolean";
				break;
			case "Double":
				reportedType="double";
			}
			elAttribute.setAttribute("type",reportedType);
			switch (attributeName) {
			case "title":
				elAttribute.setAttribute("readonly","no");
				break;
			default:
				elAttribute.setAttribute("readonly","yes");
			}
			elSchema.appendChild(elAttribute);
		}
		return elSchema;
	}

	/**
	 * Inserts attribute-named Xml elements into the specified container element for the version
	 * tuple vt.
	 * @param vt The version tuple to describe.
	 * @param el The Xml element that will contain the attributes.
	 * @return el
	 * @throws U_Exception
	 */

	public Element describeObjectIntoXmlElement(VersionsTuple vt, Element el, String uriToObject) throws U_Exception {

		Document doc = el.getOwnerDocument();
		
		Element elAttributes = doc.createElement("attributes");
		for (String attributeName : versionsTable.attributesArray()) {
			Element elAttribute = doc.createElement(attributeName);
			Object obj = vt.getAttributeValue(attributeName);
			elAttribute.appendChild(doc.createTextNode(obj==null?"":obj.toString()));
			elAttributes.appendChild(elAttribute);
		}
		el.appendChild(elAttributes);
		
		Element elReferences = doc.createElement("references");
		
		Element elReference = doc.createElement("reference");
		elReference.setAttribute("mode", "download");
		elReference.appendChild(doc.createTextNode(uriToObject+"/download?versioncount="+vt.versioncount));
		elReferences.appendChild(elReference);
		UserObjectReferences.insertIntoReferencesElement(vt, elReferences);
		
		el.appendChild(elReferences);
		return el;
	}
	
	
	public void applyToObjects(String filter, SelectionTransformer<ObjectsTuple> xform) throws U_Exception {
		try {
			objectsTable.applySelection(objectsTable.EF.parse(filter), xform);
		} catch (FilterExpressionException e) {
			throw new U_Exception(U_Exception.ERROR.ExpressionError,e);
		}
	}
	
	
	public void startTransaction() throws U_Exception {
		connection.startTransaction();
	}
	
	public void commitTransaction() throws U_Exception {
		connection.commitTransaction();
	}
	
	public void rollbackTransaction() throws U_Exception {
		connection.rollbackTransaction();
	}
	
	public boolean transactionInProgress() throws U_Exception {
		return connection.transactionInProgress();
	}
	
	/**
	 * First, normalizes all Sequence tag values, then removes any tag 
	 * values that are currently unused in the
	 * object_tags table, returning a report of actions taken.
	 * The report will also warn of tag names that have no values.
	 * 
	 * @param doc The document will be used to create
	 * a return element rooted by a {@code <report>} element which
	 * gives details of the actions taken.
	 * @param reportOnly If true, then only a report of deletions that WOULD have been
	 * made will be returned. No changes will actually be made.
	 * @return An XML {@code <report>} element;
	 */
	public Element scrubTags(Document doc,boolean reportOnly) throws U_Exception {
		if (doc==null)
			throw new U_Exception(ERROR.Unexpected,"Cannot generate report without XML document to own it");
		Element elReport = doc.createElement("report");
		try (TransactionManager tm = new TransactionManager()) {
			
			//Tag normalization pass
			Element elPass = doc.createElement("pass");
			elPass.setAttribute("phase", "1");
			elPass.setAttribute("description", (reportOnly?"Report proposed":"Perform")+" sequence value noramlization");
			elReport.appendChild(elPass);
			for (TagsTuple tag : getTags()) {
				if (tag.type.equals("Sequence")) {
					Element elTag = doc.createElement("tag");
					elTag.setAttribute("name", tag.name);
					elPass.appendChild(elTag);
					for (Object_tagsTuple ott : object_tagsTable.select(object_tagsTable.EF.comp("@name","=",tag.name))) {
						String normalized = Util.sequencify(ott.value);
						if (!normalized.equals(ott.value)) {
							Element elNormalized = 
									doc.createElement(reportOnly?"wouldnormalize":"normalized");
								elNormalized.setAttribute("oldvalue", ott.value);
								elNormalized.setAttribute("newvalue",normalized);
								elTag.appendChild(elNormalized);
								untagObject(ott.handle, ott.name, ott.value);
								createTagValue(ott.name, normalized);
								tagObject(ott.handle, ott.name, normalized);
							}
						}
					}
				}

			//Tag value deletion pass
			elPass = doc.createElement("pass");
			elPass.setAttribute("phase", "2");
			elPass.setAttribute("description", (reportOnly?"Report proposed":"Perform")+" deletion of unused tag values");
			elReport.appendChild(elPass);
			for (TagsTuple tag : getTags()) {
				Element elTag = doc.createElement("tag");
				elTag.setAttribute("name", tag.name);
				elPass.appendChild(elTag);
				for (Tag_valuesTuple tvt : getTagValues(tag.name)) {
					if (!tagValueInUse(tvt.name, tvt.value)) {
						if (reportOnly) {
							Element elWouldDelete = doc.createElement("woulddelete");
							elWouldDelete.setAttribute("value", tvt.value);
							elTag.appendChild(elWouldDelete);
						} else {
							Element elDeleted = doc.createElement("deleted");
							elDeleted.setAttribute("value", tvt.value);
							elTag.appendChild(elDeleted);
						}
						deleteTagValue(tvt.name, tvt.value);
					}
				}
			}
			
			
			if (!reportOnly)
				tm.commit();
			
		} catch (FilterExpressionException e) {
			throw new U_Exception(ERROR.Unexpected,"Invalid filter expression",e);
		} catch (U_Exception e) {
			throw e; 
		}
		return elReport;
	}
	
	public Element countTestula(Document doc) throws U_Exception {
		//Go through all the values in the tag_values table
		//and show what their normalized and sort values would be.
		Element elSequenceTags = doc.createElement("sequencetags");
		for (TagsTuple tt : getTags()) {
			if (tt.type.equals("Sequence")) {
				Element elSequenceTag = doc.createElement("sequencetag");
				elSequenceTag.setAttribute("name", tt.name);
				elSequenceTags.appendChild(elSequenceTag);
				for (Tag_valuesTuple tvt : getTagValues(tt.name)) {
					String normalized = Util.sequencify(tvt.value);
					long sort = Util.sequenceSortOrder(normalized);
					if (sort!=Long.MAX_VALUE) continue;
					Element elSequenceValue = doc.createElement("sequencevalue");
					elSequenceValue.setAttribute("value", tvt.value);
					elSequenceValue.setAttribute("normalized",normalized);
					elSequenceValue.setAttribute("sortvalue",Long.toString(sort));
					elSequenceValue.setAttribute("asbinary",Long.toBinaryString(sort));
					elSequenceTag.appendChild(elSequenceValue);
				}
			}
		}
		return elSequenceTags;
	}
	
	/**
	 * Used by instances of RepositoryManager to wrap sequences of database
	 * updates with a transaction IF a transaction isn't already in progress
	 * as dictated by the RM's client.  This class implements AutoCloseable
	 * to ensure transactions are rolledback if they are not committed and
	 * not owned by the client. Typical usage is:
	 * 
	 * <PRE>
	 * {@code
	 *    try (TransactionManager tm = new TransactionManager()) {
	 *        //body of code with tm.commit() and/or tm.rollback()
	 *    } catch (U_Exception e) {
	 *      //A U_Exception.ERROR.TransactionError will be thrown
	 *      //if the close() fails.
	 *    }
	 * </PRE>
	 *
	 */

	private class TransactionManager implements AutoCloseable {

		private boolean I_Own_This_Transaction=false;
		
		public TransactionManager() throws U_Exception {
			if (!transactionInProgress()) {
				I_Own_This_Transaction = true;
				startTransaction();
			}
		}
		
		public void commit() throws U_Exception {
			if (I_Own_This_Transaction) {
				commitTransaction();
				I_Own_This_Transaction=false;
			}
		}
		
		public void rollback() throws U_Exception {
			if (I_Own_This_Transaction) {
				rollbackTransaction();
				I_Own_This_Transaction=false;
			}
		}
		
		public void close() throws U_Exception {
			if (I_Own_This_Transaction) {
				rollbackTransaction();
			}
		}
	}
	
}