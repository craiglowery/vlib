package com.craiglowery.java.vlib.tuple;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.craiglowery.java.vlib.common.U_Exception;
import com.craiglowery.java.vlib.common.U_Exception.ERROR;

/**
 * This is a derived class that implements PersistenceConnection for
 * Postgresql datbases.
 *
 */
public class PostgresqlConnection extends PersistenceConnection {
	
	public Connection db=null;
	
	/**
	 * Creates a new instance of the VLIB database adapter.
	 */
	public PostgresqlConnection(
			String hostname, 
			String databasename, 
			String username, 
			String password) 
			throws U_Exception
	{			
		//constraintViolationDetectionPattern = null;
		try {
			Class.forName("org.postgresql.Driver");  //Force load of the postgres driver
			String DBURL = String.format("jdbc:postgresql://%s/%s",hostname,databasename);
			db = DriverManager.getConnection(DBURL, username, password);
		} catch (SQLException | ClassNotFoundException e) {
			throw new U_Exception(U_Exception.ERROR.DatabaseError,"Unable to connect to to Postgres database",e);
		}
	}

	private void checkOpen() throws U_Exception {
		if (db==null) throw new U_Exception(U_Exception.ERROR.DatabaseError,"The database connection is not open");
	}
	
	public void startTransaction() throws U_Exception {
		checkOpen();
		if (transactionInProgress())
			throw new U_Exception(U_Exception.ERROR.QueryError,"Transaction already in progress");
		try {
			db.setAutoCommit(false);
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"Could not start transaction",e);
		}
	}
	
	public void commitTransaction() throws U_Exception {
		checkOpen();
		if (!transactionInProgress())
			throw new U_Exception(U_Exception.ERROR.QueryError,"No transaction in progress");
		try {
			db.commit();
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"Could not commit transaction",e);
		}
		try {
			db.setAutoCommit(true);
		} catch (SQLException e) {
			/* Ignore */
		}
	}
	
	public void rollbackTransaction() throws U_Exception {
		checkOpen();
		if (!transactionInProgress())
			throw new U_Exception(U_Exception.ERROR.TransactionError,"No transaction in progress");
		try {
			db.rollback();
		} catch (SQLException e) {
			throw new U_Exception(U_Exception.ERROR.TransactionError,"Could not rollback transaction",e);
		}
		try {
			db.setAutoCommit(true);
		} catch (SQLException e) {
			/* ignore */
		}
	}
	
	/**
	 * Checks to see if a transaction block is currently open (active)
	 * 
	 * @return True if a transaction is in progress.
	 */
	public boolean transactionInProgress()
		throws U_Exception
	{
		try {
			return !db.getAutoCommit();
		} catch (SQLException se) {
			throw new U_Exception(U_Exception.ERROR.QueryError,"SQL Error: "+se.getMessage());
		}
	}
	
	/**
	 * Closes the connection.
	 * 
	 * If a transaction is in progress, it is rolled back silently although the fact may be logged. 
	 */
	public void close()
	{
		if (db!=null)
		try {
			if (transactionInProgress())
				db.rollback();
			db.close();
			db=null;
		} catch (Exception se) {
				//Log here
		}
	}

	@Override
	public boolean isValid() throws U_Exception {
		try {
			return db.isValid(5);
		} catch (Exception e) {
			throw new U_Exception(ERROR.Unexpected,e);
		}
	}
}