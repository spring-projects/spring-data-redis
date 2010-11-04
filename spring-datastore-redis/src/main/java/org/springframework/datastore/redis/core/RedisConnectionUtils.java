/*
 * Copyright 2006-2009 the original author or authors.
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
package org.springframework.datastore.redis.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.datastore.redis.core.connection.RedisConnection;
import org.springframework.datastore.redis.core.connection.RedisConnectionFactory;
import org.springframework.transaction.support.ResourceHolder;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * Helper class featuring {@link RedisConnection} handling, allowing for reuse of instances within transactions.
 * 
 * @author Costin Leau
 */
public abstract class RedisConnectionUtils {

	private static final Log log = LogFactory.getLog(RedisConnectionUtils.class);

	public static RedisConnection<?> getRedisConnection(RedisConnectionFactory factory) {
		return doGetRedisConnection(factory, true);
	}

	public static RedisConnection<?> doGetRedisConnection(RedisConnectionFactory factory, boolean allowCreate) {
		Assert.notNull(factory, "No RedisConnectionFactory specified");

		RedisConnectionHolder pmHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(factory);
		//TODO: investigate tx synchronization

		if (pmHolder != null)
			return pmHolder.getConnection();

		if (log.isDebugEnabled())
			log.debug("Opening RedisConnection");

		RedisConnection<?> conn = factory.getConnection();

		if (TransactionSynchronizationManager.isSynchronizationActive()) {
			pmHolder = new RedisConnectionHolder(conn);
			TransactionSynchronizationManager.registerSynchronization(new RedisConnectionSynchronization(pmHolder,
					factory, true));
			TransactionSynchronizationManager.bindResource(factory, pmHolder);

		}
		return pmHolder.getConnection();

	}

	public static void releaseConnection(RedisConnection<?> conn, RedisConnectionFactory factory) {
		if (conn == null) {
			return;
		}
		// Only release non-transactional/non-bound connections.
		if (!isConnectionTransactional(conn, factory)) {
			log.debug("Closing Redis Connection");
			conn.close();
		}
	}

	public static boolean isConnectionTransactional(RedisConnection<?> conn, RedisConnectionFactory connFactory) {
		if (connFactory == null) {
			return false;
		}
		RedisConnectionHolder connHolder = (RedisConnectionHolder) TransactionSynchronizationManager.getResource(connFactory);
		return (connHolder != null && conn == connHolder.getConnection());
	}

	private static class RedisConnectionSynchronization extends
			ResourceHolderSynchronization<RedisConnectionHolder, RedisConnectionFactory> {

		private final boolean newRedisConnection;

		public RedisConnectionSynchronization(RedisConnectionHolder connHolder, RedisConnectionFactory connFactory,
				boolean newRedisConnection) {
			super(connHolder, connFactory);
			this.newRedisConnection = newRedisConnection;
		}

		@Override
		protected boolean shouldUnbindAtCompletion() {
			return this.newRedisConnection;
		}

		@Override
		protected void releaseResource(RedisConnectionHolder resourceHolder, RedisConnectionFactory resourceKey) {
			releaseConnection(resourceHolder.getConnection(), resourceKey);
		}
	}

	private static class RedisConnectionHolder implements ResourceHolder {

		private boolean isVoid = false;
		private final RedisConnection<?> conn;

		public RedisConnectionHolder(RedisConnection<?> conn) {
			this.conn = conn;
		}

		@Override
		public boolean isVoid() {
			return isVoid;
		}

		public RedisConnection<?> getConnection() {
			return conn;
		}

		@Override
		public void reset() {
			// no-op
		}

		@Override
		public void unbound() {
			this.isVoid = true;
		}
	}
}