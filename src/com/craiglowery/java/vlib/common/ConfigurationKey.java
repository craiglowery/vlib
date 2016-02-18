package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
/** Keys used to retrieve configuration information. **/
public enum ConfigurationKey {
	
	//---- database and back-end persistence keys
	
	/** The hostname of the database server, to be used with PersistenceConnection.
	 * REQUIRED. **/
	@RequiredConfigurationKey DB_HOSTNAME,
	
	/** The name of the database to open on the host.
	 * REQUIRED. **/
	@RequiredConfigurationKey DB_NAME,
	
	/** The string identifier for the algorithm to use in encrypting 
	 * database credentials.  The credentials are stored in the 
	 * {@code DB_AUTH_PWDFILE}, encrypted using this algorithm and the
	 * key {@code DB_AUTH_ALGORITHM_KEY}. OPTIONAL.
	 * See the {@code Crypto} class. */
	@SecureConfigurationValue
	@DefaultConfigurationValue("AES/ECB/PKCS5Padding") DB_AUTH_ALGORITHM,
	
	/** The key to use in encrypting and decrypting database credentials.  It is
	 * important that the configurtion file is locked down so that only the
	 * server can read it, in order to protect this key. 
	 * See the {@code Crypto} class. REQUIRED.*/
	@SecureConfigurationValue
	@RequiredConfigurationKey DB_AUTH_ALGORITHM_KEY,		
	
	/** The absolute path to the file where encrypted database credentials are to
	 * be stored. See the {@code Crypto} class. REQUIRED.
	 */
	@SecureConfigurationValue
	@RequiredConfigurationKey DB_AUTH_PWDFILE,
	
	//---- Repository related keys
	
	/** Absolute path name of the root of the entire repository. 
	 * REQUIRED. **/
	@RequiredConfigurationKey DIR_REPO_ROOT,

	/** Absolute path to subdirectory of {@code DIR_REPO_ROOT}
	 * which serves as the library subdirectory.
	 * OPTIONAL.
	 */
	@DefaultConfigurationValue("{$DIR_REPO_ROOT}/lib") SUBDIR_REPO_LIB,

	/** Absolute path to subdirectory of {@code DIR_REPO_ROOT}
	 * to which uploaded material
	 * is placed prior to being imported.
	 * OPTIONAL.	
	 */
	@DefaultConfigurationValue("{$SUBDIR_REPO_LIB}/upload") SUBDIR_REPO_UPLOAD,
	
	/** Absolute path to subdirectory of  {@code DIR_REPO_LIB}
	 * to which retired material
	 * is placed after being deleted from the visible portion of the repository.
	 * OPTIONAL.
	 */
	@DefaultConfigurationValue("{$SUBDIR_REPO_LIB}/trash") SUBDIR_REPO_TRASH,
	
	/** Absolute path to subdirectory of  {@code DIR_REPO_LIB}
	 * to which temporary files may be written or linked from other 
	 * parts of the repository.
	 * OPTIONAL.
	 */
	@DefaultConfigurationValue("{$SUBDIR_REPO_LIB}/tmp")SUBDIR_REPO_TEMP,
	
	/** The name of the directory beneath {@code DIR_REPO_ROOT}
	 * where incoming video files are placed in the legacy file name space.
	 * OPTIONAL.
	 */
	@DefaultConfigurationValue("{$DIR_REPO_ROOT}/Incoming") SUBDIR_REPO_INCOMING,
	
	/** The regular expression that is used to test for valid video file
	 * extensions.  OPTIONAL.
	 */
	@DefaultConfigurationValue("\\.(avi|divx|m2ts|xvid|mpeg|mpg|mp4|3g2|mkv)$") VIDEO_EXTENSIONS_REGEX,	
	
	/** The number of seconds after which an upload resource is created before it is
	 * considered to be stale.  This is primarily used by cleaning routines that
	 * remove stale URs. OPTIONAL.
	 */
	@DefaultConfigurationValue("43200")	UPLOAD_RESOURCE_LIFESPAN,

	/** General use temporary directory that does not have to reside on the same
	 * file system as the repository. OPTIONAL.
	 */
	@DefaultConfigurationValue("/tmp") DIR_TEMP,		
	
	/** External shell command to which a file's path name can be appended
	 * and then executed by the shell to write the checksum for the file to
	 * {@code stdout}.
	 * OPTIONAL. 
	 */
	@DefaultConfigurationValue("/usr/bin/sha1sum") EXTERNAL_CHECKSUM_COMMAND,
	
	/** External shell command to which a file's path name can be appended
	 * and then executed by the shell to write the inode number and link count
	 * as two integers separated by a space on a single line to the {@code stdout}. OPTIONAL.
	 */
	@DefaultConfigurationValue("/usr/bin/stat -c '%i %h'") EXTERNAL_STAT_COMMAND,
	
	/** The SMB (Samba) university path equivalent to DIR_REPO_ROOT, that can
	 * be accessed over a Windows LAN.
	 */
	@RequiredConfigurationKey SAMBA_PATH,
	
	/** The maximum number of rows that a TableAdapter can return from a selection
	 * function returning a {@code List<T>}.
	 */
	@DefaultConfigurationValue("10000") MAX_TABLE_RESULT_SIZE,
	
	/** A custom program that accepts four parameters:<p>
	 * <ol>
	 *    <li> The path to the root of the repository.  The value of {@code DIR_REPO_ROOT}
	 *         will be passed.
	 *    <li> The name of the library subdirectory.  The value of {@code SUBDIR_REPO_LIB}
	 *         will be passed.
	 *    <li> An integer, which is an inode number of an outdated content file
	 *         that is being replaced by new content in the library.
	 *    <li> The path name to the new file in the {@code SUBDIR_REPO_LIB} directory
	 *         where the new content can be found.
	 * </ol>
	 * 
	 * This program should perform a depth-first traversal of the repository starting
	 * at the root (the first parameter), but should prune traversal of the library
	 * subdirectory (the second parameter).  For each link of the inode (the third
	 * parameter) encountered, it should replace the link with a new link of the same
	 * path name linked to the new file (the fourth parameter).  In this way, content
	 * that has been updated for an object in the repository can be updated in legacy
	 * space by using hard links in *NIX systems, instead of actually copying the
	 * material.  Since this is *NIX specific and requires linking (not a Java concept),
	 * an external program was required. There is no default.<p>
	 */
	@RequiredConfigurationKey EXTERNAL_SEEK_AND_REPLACE_COMMAND,
	
	@DefaultConfigurationValue("1000000000") MAX_FILE_UPLOAD_SIZE
}
