package com.craiglowery.java.vlib.common;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 * Uniform exception for the vlib project, allows us to pass the same
 * exception through several layers without having to map to and throw
 * an exception from that particular level.
 */
public class U_Exception extends Exception {
	
	public static final boolean ENABLE_LOGGING = true;
	
	private static final long serialVersionUID = -2370911617359110164L;
	
	public static Logger logger;
	
	static void initialize() {
		ConfigureLogger();
	}

	static public void ConfigureLogger() {
		if (ENABLE_LOGGING && logger==null) {
			logger = Logger.getLogger(U_Exception.class);
			logger.setLevel(Level.WARN);
			logger.addAppender(new ConsoleAppender(new SimpleLayout()));
		}
	}
	
	public enum ERROR {
			/** There is no object with that handle */
			NoSuchHandle,
			/** The database connection returned an error */
			DatabaseError,
			/** The SQL driver returned an unexpected error */
			TransactionError,
			/** There was a problem managing a transaction on the database */
			QueryError,
			/** The database is in an inconsistent state and should be repaired */
			InconsistentDatabase,
			/** There is no version of the object with that handle and timestamp */
			NoSuchVersion,
			/** Something happened that is very unusual or unlikely */
			Unexpected,
			/** File rename failed */
			FileRenameFailed,
			/** Invalid tag name */
			InvalidTagName,
			/** Invalid tag value */
			InvalidTagValue,
			/** The name=value pair does not exist */
			NoSuchTagValuePair,
			/** Tag is in use */
			TagInUse,
			/** An error occurred during parsing of a string expression or query */
			ParserError,
			/** Configuration error */
			ConfigurationError,
			/** A task couldn't be completed after a certain amount of time or attempts */
			Timeout,
			/** File does not exist */
			NoSuchFile,
			/** The file suffix is not recognized */
			UnrecognizedFileSuffix,
			/** An error occurred while manipulating a file */
			FileError,
			/** Another file has the same length and sha1sum, or was copied from the same source */
			PotentialDuplicate,
			/** The filename is illegal */
			InvalidFilename,
			/** An error occurred while reading or writing a file */
			IOError,
			/** Error occured during computation of checksum */
			EncryptionError,
			/** An argument's value is not allowed */
			BadParameter,
			/** A requested action is not allowed */
			IllegalRequest,
			/** An object did ot pass health check or database validation */
			ValidationError,
			/** The reason for the error is unknown */
			Unknown,
			/** The session is not valid */
			InvalidSession,
			/** Type mismatch */
			TypeMismatch,
			/** There is no field of this name in the tuple */
			NoSuchField,
			/** An invalid expression or expression operation occurred */
			ExpressionError,
			/** The tag name does not exist */
			NoSuchTagName,
			/** Database constraint violation */
			ConstraintViolation
		};
	
		
	public ERROR errorCode;
		
	public U_Exception(ERROR errorCode)  {
		super();
		this.errorCode=errorCode;
		if (ENABLE_LOGGING) logger.log(Level.ERROR, null, this);
	}

	public U_Exception(Level logLevel, ERROR errorCode) {
		super();
		this.errorCode=errorCode;
		if (ENABLE_LOGGING) logger.log(logLevel,null, this);
	}
	
	public U_Exception(ERROR errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
		if (ENABLE_LOGGING) logger.log(Level.ERROR, message, this);
	}
	
	public U_Exception(Level logLevel, ERROR errorCode, String message) {
		super(message);
		this.errorCode = errorCode;
		if (ENABLE_LOGGING) logger.log(logLevel, message, this);
	}
	
	public U_Exception(ERROR errorCode, String message, Exception inner) {
		super(message,inner);		
		this.errorCode = errorCode;
		if (ENABLE_LOGGING) logger.log(Level.ERROR, message, this);
	}
	
	public U_Exception(Level logLevel, ERROR errorCode, String message, Exception inner) {
		super(message,inner);		
		this.errorCode = errorCode;
		if (ENABLE_LOGGING) logger.log(logLevel,message,this);
	}
	
	public U_Exception(ERROR errorCode, Exception inner) {
		super(inner);		
		this.errorCode = errorCode;
		if (ENABLE_LOGGING) logger.log(Level.ERROR,null,this);
	}

	public U_Exception(Level logLevel, ERROR errorCode, Exception inner) {
		super(inner);		
		this.errorCode = errorCode;
		if (ENABLE_LOGGING) logger.log(logLevel, null, this);
	}

	public String thrownBy() {
		StackTraceElement[] st = getStackTrace();
		return st.length<1 ? "(unknown source)":st[0].getMethodName();
	}
	
	public String getMessage() {
		return String.format("%s at %s: %s", errorCode.name(),thrownBy(),super.getMessage());
	}

}
