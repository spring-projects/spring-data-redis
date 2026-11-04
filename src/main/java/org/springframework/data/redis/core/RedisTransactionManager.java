/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils.RedisConnectionHolder;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.transaction.PlatformTransactionManager} implementation for a single
 * {@link RedisConnectionFactory}. The manager binds a connection to the current thread and uses Redis {@code MULTI},
 * {@code EXEC}, and {@code DISCARD} to manage a transaction.
 * <p>
 * Redis transactions do not provide rollback after {@code EXEC} has started applying commands. A rollback only discards
 * commands that are still queued in the Redis transaction.
 *
 * @author Alpha
 * @since 4.2
 */
public class RedisTransactionManager extends AbstractPlatformTransactionManager implements ResourceTransactionManager {

	private final RedisConnectionFactory connectionFactory;

	/**
	 * Create a new {@link RedisTransactionManager} for the given {@link RedisConnectionFactory}.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 */
	public RedisTransactionManager(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null");
		this.connectionFactory = connectionFactory;
	}

	@Override
	public RedisConnectionFactory getResourceFactory() {
		return this.connectionFactory;
	}

	@Override
	protected Object doGetTransaction() throws TransactionException {

		RedisTransactionObject transaction = new RedisTransactionObject();
		transaction.setConnectionHolder((RedisConnectionHolder) TransactionSynchronizationManager
				.getResource(this.connectionFactory));

		return transaction;
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) {

		RedisConnectionHolder connectionHolder = ((RedisTransactionObject) transaction).getConnectionHolder();

		return connectionHolder != null && connectionHolder.isTransactionActive();
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {

		if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
			throw new InvalidIsolationLevelException("Redis does not support custom isolation levels");
		}

		RedisTransactionObject transactionObject = (RedisTransactionObject) transaction;
		RedisConnectionHolder connectionHolder = transactionObject.getConnectionHolder();
		boolean newConnectionHolder = connectionHolder == null;
		boolean previousSynchronizationState = connectionHolder != null
				&& connectionHolder.isSynchronizedWithTransaction();

		RedisConnection connection = null;

		try {
			if (newConnectionHolder) {
				connection = this.connectionFactory.getConnection();
				Assert.state(connection != null, "RedisConnectionFactory returned a null connection");

				if (!definition.isReadOnly()) {
					connection = RedisConnectionUtils.createConnectionSplittingProxy(connection, this.connectionFactory);
				}

				connectionHolder = new RedisConnectionHolder(connection);
				connectionHolder.requested();
				transactionObject.setConnectionHolder(connectionHolder);
			} else {
				connectionHolder.requested();
				connection = connectionHolder.getRequiredConnection();
			}

			if (!definition.isReadOnly()) {
				connection.multi();
			}

			connectionHolder.setTransactionActive(true);
			connectionHolder.setSynchronizedWithTransaction(true);
			transactionObject.setNewConnectionHolder(newConnectionHolder);
			transactionObject.setReadOnly(definition.isReadOnly());
			transactionObject.setPreviousSynchronizationState(previousSynchronizationState);

			if (newConnectionHolder) {
				TransactionSynchronizationManager.bindResource(this.connectionFactory, connectionHolder);
			}
		} catch (RuntimeException ex) {
			if (newConnectionHolder) {
				if (connectionHolder != null) {
					if (TransactionSynchronizationManager.getResource(this.connectionFactory) == connectionHolder) {
						TransactionSynchronizationManager.unbindResource(this.connectionFactory);
					}
					connectionHolder.setTransactionActive(false);
					connection = connectionHolder.getConnection();
					connectionHolder.setConnection(null);
					connectionHolder.clear();
				}
				transactionObject.setConnectionHolder(null);
				if (connection != null) {
					RedisConnectionUtils.releaseConnection(connection, this.connectionFactory);
				}
			} else if (connectionHolder != null) {
				connectionHolder.released();
			}

			throw new CannotCreateTransactionException("Could not start Redis transaction", ex);
		}
	}

	@Override
	protected Object doSuspend(Object transaction) throws TransactionException {

		RedisTransactionObject transactionObject = (RedisTransactionObject) transaction;
		RedisConnectionHolder connectionHolder = transactionObject.getConnectionHolder();

		if (connectionHolder == null) {
			return null;
		}

		TransactionSynchronizationManager.unbindResource(this.connectionFactory);
		transactionObject.setConnectionHolder(null);

		return connectionHolder;
	}

	@Override
	protected void doResume(@Nullable Object transaction, Object suspendedResources) throws TransactionException {

		if (transaction == null || suspendedResources == null) {
			return;
		}

		RedisConnectionHolder connectionHolder = (RedisConnectionHolder) suspendedResources;
		TransactionSynchronizationManager.bindResource(this.connectionFactory, connectionHolder);
		((RedisTransactionObject) transaction).setConnectionHolder(connectionHolder);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) throws TransactionException {

		RedisTransactionObject transaction = (RedisTransactionObject) status.getTransaction();

		if (!transaction.isReadOnly()) {
			transaction.getRequiredConnectionHolder().getRequiredConnection().exec();
		}
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) throws TransactionException {

		RedisTransactionObject transaction = (RedisTransactionObject) status.getTransaction();

		if (!transaction.isReadOnly()) {
			transaction.getRequiredConnectionHolder().getRequiredConnection().discard();
		}
	}

	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {

		RedisTransactionObject transaction = (RedisTransactionObject) status.getTransaction();
		transaction.getRequiredConnectionHolder().setRollbackOnly();
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {

		RedisTransactionObject transactionObject = (RedisTransactionObject) transaction;
		RedisConnectionHolder connectionHolder = transactionObject.getConnectionHolder();

		if (connectionHolder == null) {
			return;
		}

		if (transactionObject.isNewConnectionHolder()) {
			RedisConnection connection = connectionHolder.getConnection();

			if (TransactionSynchronizationManager.getResource(this.connectionFactory) == connectionHolder) {
				TransactionSynchronizationManager.unbindResource(this.connectionFactory);
			}

			connectionHolder.setConnection(null);
			connectionHolder.clear();

			if (connection != null) {
				RedisConnectionUtils.releaseConnection(connection, this.connectionFactory);
			}
		} else {
			connectionHolder.setTransactionActive(false);
			connectionHolder.setSynchronizedWithTransaction(transactionObject.wasSynchronizedWithTransaction());
			connectionHolder.resetRollbackOnly();
			connectionHolder.released();
		}

		transactionObject.setConnectionHolder(null);
	}

	private static class RedisTransactionObject implements SmartTransactionObject {

		@Nullable private RedisConnectionHolder connectionHolder;

		private boolean newConnectionHolder;

		private boolean readOnly;

		private boolean previousSynchronizationState;

		public void setConnectionHolder(@Nullable RedisConnectionHolder connectionHolder) {
			this.connectionHolder = connectionHolder;
		}

		public @Nullable RedisConnectionHolder getConnectionHolder() {
			return this.connectionHolder;
		}

		public RedisConnectionHolder getRequiredConnectionHolder() {

			RedisConnectionHolder connectionHolder = getConnectionHolder();

			if (connectionHolder == null) {
				throw new IllegalStateException("No Redis ConnectionHolder available");
			}

			return connectionHolder;
		}

		public void setNewConnectionHolder(boolean newConnectionHolder) {
			this.newConnectionHolder = newConnectionHolder;
		}

		public boolean isNewConnectionHolder() {
			return this.newConnectionHolder;
		}

		public void setReadOnly(boolean readOnly) {
			this.readOnly = readOnly;
		}

		public boolean isReadOnly() {
			return this.readOnly;
		}

		public void setPreviousSynchronizationState(boolean previousSynchronizationState) {
			this.previousSynchronizationState = previousSynchronizationState;
		}

		public boolean wasSynchronizedWithTransaction() {
			return this.previousSynchronizationState;
		}

		@Override
		public boolean isRollbackOnly() {
			return this.connectionHolder != null && this.connectionHolder.isRollbackOnly();
		}

		@Override
		public void flush() {
			// Redis transactions do not support flushing queued commands before commit.
		}
	}

}
