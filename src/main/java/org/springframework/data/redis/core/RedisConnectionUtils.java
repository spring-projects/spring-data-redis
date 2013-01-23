/*
 * Copyright 2010-2011-2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.transaction.support.ResourceHolder;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Helper class featuring {@link RedisConnection} handling, allowing for reuse of instances within 'transactions'/scopes.
 * 
 * @author Costin Leau
 */
public abstract class RedisConnectionUtils {

	private static final Log log = LogFactory.getLog(RedisConnectionUtils.class);

	/**
	 * Binds a new Redis connection (from the given factory) to the current thread, if none is already bound. 
	 * 
	 * @param factory connection factory
	 * @return a new Redis connection
	 */
	public static RedisConnection bindConnection(RedisConnectionFactory factory) {
		return doGetConnection(factory, true, true);
	}

	/**
	 * Gets a Redis connection from the given factory. Is aware of and will return any existing corresponding connections bound to the current thread, 
	 * for example when using a transaction manager. Will always create a new connection otherwise.
	 * 
	 * @param factory connection factory for creating the connection
	 * @return an active Redis connection
	 */
	public static RedisConnection getConnection(RedisConnectionFactory factory) {
		return doGetConnection(factory, true, false);
	}

	/**
	 * Gets a Redis connection. Is aware of and will return any existing corresponding connections bound to the current thread, 
	 * for example when using a transaction manager. Will create a new Connection otherwise, if {@code allowCreate} is <tt>true</tt>.
	 * 
	 * @param factory connection factory for creating the connection
	 * @param allowCreate whether a new (unbound) connection should be created when no connection can be found for the current thread
	 * @param bind binds the connection to the thread, in case one was created
	 * @return an active Redis connection
	 */
	public static RedisConnection doGetConnection(RedisConnectionFactory factory, boolean allowCreate, boolean bind) {
		Assert.notNull(factory, "No RedisConnectionFactory specified");

		RedisConnectionHolder connHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(factory);
		//TODO: investigate tx synchronization

		if (connHolder != null)
			return connHolder.getConnection();

		if (!allowCreate) {
			throw new IllegalArgumentException("No connection found and allowCreate = false");
		}

		if (log.isDebugEnabled())
			log.debug("Opening RedisConnection");

		RedisConnection conn = factory.getConnection();

		if (bind) {
			connHolder = new RedisConnectionHolder(conn);
			TransactionSynchronizationManager.bindResource(factory, connHolder);
			return connHolder.getConnection();
		}
		return conn;
	}

	/**
	 * Closes the given connection, created via the given factory if not managed externally (i.e. not bound to the thread).
	 * 
	 * @param conn the Redis connection to close
	 * @param factory the Redis factory that the connection was created with
	 */
	public static void releaseConnection(RedisConnection conn, RedisConnectionFactory factory) {
		if (conn == null) {
			return;
		}
		// Only release non-transactional/non-bound connections.
		if (!isConnectionTransactional(conn, factory)) {
			if (log.isDebugEnabled()) {
				log.debug("Closing Redis Connection");
			}
			conn.close();
		}
	}

	/**
	 * Unbinds and closes the connection (if any) associated with the given factory.
	 * 
	 * @param factory Redis factory
	 */
	public static void unbindConnection(RedisConnectionFactory factory) {
		RedisConnectionHolder connHolder = (RedisConnectionHolder) TransactionSynchronizationManager.unbindResourceIfPossible(factory);
		if (connHolder != null) {
			RedisConnection connection = connHolder.getConnection();
			connection.close();
		}
	}

	/**
	 * Return whether the given Redis connection is transactional, that is, bound to the current thread by Spring's transaction facilities. 
	 * 
	 * @param conn Redis connection to check
	 * @param connFactory Redis connection factory that the connection was created with
	 * @return whether the connection is transactional or not
	 */
	public static boolean isConnectionTransactional(RedisConnection conn, RedisConnectionFactory connFactory) {
		if (connFactory == null) {
			return false;
		}
		RedisConnectionHolder connHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(connFactory);
		return (connHolder != null && conn == connHolder.getConnection());
	}

	private static class RedisConnectionHolder implements ResourceHolder {

		private boolean isVoid = false;
		private final RedisConnection conn;

		public RedisConnectionHolder(RedisConnection conn) {
			this.conn = conn;
		}

		
		public boolean isVoid() {
			return isVoid;
		}

		public RedisConnection getConnection() {
			return conn;
		}

		
		public void reset() {
			// no-op
		}

		
		public void unbound() {
			this.isVoid = true;
		}
	}
}