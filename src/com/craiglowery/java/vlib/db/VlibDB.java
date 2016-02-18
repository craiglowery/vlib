package com.craiglowery.java.vlib.db;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */

import java.sql.*;
import java.util.regex.PatternSyntaxException;

/**
 * Wraps a JDBC adapter for the VLIB database intension.  
 * 
 */
public class VlibDB implements AutoCloseable {

	/*
	 * Thread-safe considerations:
	 * 
	 * The same regex Pattern for recognizing constraint violations in message strings
	 * is an immutable object and is thread-safe.  The code that compiles the pattern
	 * on first-use need not be synchronized because in the unlikely chance a race
	 * condition results in the pattern being compiled twice and the first instance
	 * being replaced by the second in the static reference, they're the same, so there
	 * is no perceived difference.
	 * 
	 * Instances of VlibDB are intended to be used in a single thread.  If multiple
	 * threads need a database connection, they are expected to have their own, so
	 * synchronization of individual instances is not addressed.
	 * 
	 * It implements AutoCloseable although there are no resources to release. This is 
	 * to enforce good usage of it as a resource by clients (in try-using blocks).  
	 */
	
	/** This pattern will be built upon first use and then retained using the regex String below. */
	private static java.util.regex.Pattern constraintViolationDetectionPattern;
	
	/** The pattern looks for the word "violates" and later "constraint" then something in double quotes, which is captured */
	private final String CONSTRAINT_VIOLATION_DETECTION_RE = "violates .* constraint \"([\\p{Alnum}_]+)\"";

	/** The database connection to use during the lifetime of an instance. */
	private Connection db;
	
	/**
	 * Creates a new instance of the VLIB database adapter.
	 */
	public VlibDB(String hostname, String databasename, String username, String password) 
			throws VlibDBException 
	{			
		constraintViolationDetectionPattern = null;
		try {
			//Class.forName("org.postgresql.Driver");  //Force load of the postgres driver

			String DBURL = String.format("jdbc:postgresql://%s/%s",hostname,databasename);
			db = DriverManager.getConnection(DBURL, username, password);
		} catch (SQLException e) {
			throw new VlibDBException("Unable to connect to to postgres database",e);
		}
	}
	
	/** 
	 * Begins a new transaction block.  Only one transaction block can be active at a time.
	 * nested blocks are not supported.
	 * 
	 * @param autoCloseAction Either COMMIT or ROLLBACK, is the default action to take if the
	 *                   transaction is automatically closed (such as when used in a try-using
	 *                   block) rather than explicitly committed or rolledback.
	 * @throws VlibDBException
	 */
	public Transaction createTransaction(ClosingActions autoCloseAction)
		throws VlibDBException
	{
		synchronized (this) {
			// The constructor will check if there is another transaction already active.
			return new Transaction(autoCloseAction);
		}
	}
	
	/**
	 * Checks to see if a transaction block is currently open (active)
	 * 
	 * @return True if a transaction is in progress.
	 */
	public boolean transactionActive()
		throws VlibDBException
	{
		try {
			return !db.getAutoCommit();
		} catch (SQLException se) {
			throw new VlibDBException("SQL Error: "+se.getMessage());
		}
	}
	
	/**
	 * Gets a SQL statement object for use in creating simple queries.
	 * 
	 * @return The statement object.
	 * @throws VlibDBException
	 */
	public Statement getStatement() 
			throws VlibDBException {
		try {
			return db.createStatement();
		} catch (SQLException e) {
			throw new VlibDBException("Could not create SQL statement",e);
		}
	}

	/**
	 * Gets a Prepared SQL statement object for use in easily creating parameterized statements.
	 * 
	 * @param query The query string, using '?' characters as placeholders for parameters.
	 * @return The statement object
	 * @throws VlibDBException
	 */
	public PreparedStatement getPreparedStatement(String query) 
			throws VlibDBException {
		try {
			return db.prepareStatement(query);
		} catch (SQLException e) {
			throw new VlibDBException("Could not create SQL prepared statement",e);
		}
	}
	

	/**
	 * Closes the connection.
	 * 
	 * If a transaction is in progress, it is rolled back silently although the fact may be logged. 
	 */
	public void close()
	{
		try {
			if (transactionActive())
				db.rollback();
			db.close();
		} catch (Exception se) {
				//Log here
		}
	}
	

	/**
	 *	Searches a message returned by Postgresql for a substring indicating a constraint was
	 *  violated.  If constraintName is null, then the name of the key constraint is ignored (not
	 *  checked) and any key constraint will result in TRUE being returned.
     *
	 * @param message The exception message returned by Postgresql
	 * @param constraintName The constraint name to be tested for
	 * @return True if the  constraintName was violated, or if any constraint was violated and constraintName is null
	 */
	public boolean violatesConstraint(String message, String constraintName) 
	{

		//Note: The below is thread-safe because Pattern's are immutable, and if we happen to compile it twice
		//      it's no big deal.
		if (constraintViolationDetectionPattern==null)
			try {
				constraintViolationDetectionPattern = java.util.regex.Pattern.compile(CONSTRAINT_VIOLATION_DETECTION_RE);
			} catch (PatternSyntaxException pse) {
				System.err.println("Unable to construct regular expression matcher for constraint violation detection in VlibDB: "+pse.getMessage());
			}
	
		//Perform match
		java.util.regex.Matcher matcher = constraintViolationDetectionPattern.matcher(message);
		//If the pattern was't found, then no constraint was violated
		if (!matcher.find()) 
			return false;
		//A constraint was violated.  If they didn't ask us to check which one, then return true
		if (constraintName==null) 
			return true;
		//A constraint was violated, but we only return true if it is the one they are interested in
		return constraintName.equals(matcher.group(1));
	}
	

	public void closeAnyOpenTransactions() {
		try {
			if (transactionActive())
				db.rollback();
		} catch (Exception e) {
			// ignore
		}
	}

//------------------------------------------------------------------------------------------------------------------
//-- Transaction subclass
//------------------------------------------------------------------------------------------------------------------
	
	/** Holds the serial number of the most recent (and potentially currently open) transaction */
	private static long transactionSerialNumberCounter = 0L;	
	
	/** These enum values represent the actions possible automatically on autoclose of a transaction */
	public enum ClosingActions {COMMIT, ROLLBACK};
	
	/**
	 * A wrapper class for transaction management on the database connection.  Provides an abstraction that makes
	 * transactions look like resources, and they can be used in try-using blocks.  Constructing a new Transaction
	 * instance opens a transaction on the db connection. If the Transaction autocloses (as in a try-using where it 
	 * is not explicitly closed), then the autoclose action (COMMIT, or ROLLBACK) is automatically performed. <p>
	 * 
	 * Typical usage in code looks like this:<p>
	 * 
	 * <pre>
	 *    try (Transaction t = new db.createTransaction(VlibDB.ClosingActions.ROLLBACK) {
	 *       ...
	 *       }
	 * </pre>
	 * 
	 *  The above code opens a transaction on db.  If t is not closed in the body of the try,
	 *  then it will be automatically rolled back upon exit from the block.
	 * 
	 *
	 */
	public class Transaction implements AutoCloseable {
		

		public ClosingActions autoCloseAction;
		private long serialNumber;
		
		/**
		 * Create a new transaction with the specified auto closing action.  If the transaction is autoclosed,
		 * then the autoCloseAction will be automatically applied.
		 * @param autoCloseAction Either COMMIT or ROLLBACK.
		 * @throws VlibDBException
		 */
		public Transaction(ClosingActions autoCloseAction) 
			throws VlibDBException
		{
			synchronized (db) {
				if (transactionActive())
					throw new VlibDBException("Unable to open a new transaction - another transaction is active");
				this.autoCloseAction = autoCloseAction;
				serialNumber= ++transactionSerialNumberCounter;
				try {
					db.setAutoCommit(false);
				} catch (SQLException e) {
					throw new VlibDBException("Unable to create transaction - SQL error "+e.getMessage());
				}
			}
		}
		
		/**
		 * Close the transaction with a specific disposition to COMMIT or ROLLBACK.
		 * @param closingAction Either COMMIT or ROLLBACK.
		 */
		public void close(ClosingActions closingAction) {
			try {
				synchronized (db) {
					// Is this the "current" transaction, and is it still active?
					if (serialNumber==transactionSerialNumberCounter && transactionActive()) {
						// Perform the auto closing action
						switch (closingAction) {
							case COMMIT:
								db.commit();
								break;
							case ROLLBACK: 
								db.rollback();
								break;			
						} //Switch
						db.setAutoCommit(true);
					}//If
				} //Sync
			} catch (Exception e) {
					//ignore
			}

		}
		/**
		 * Close the transaction and apply the disposition specified when the transaction was constructed.
		 */
		public void close(/* performs auto close action */) {
			close(autoCloseAction);
		}
	}



	
}

