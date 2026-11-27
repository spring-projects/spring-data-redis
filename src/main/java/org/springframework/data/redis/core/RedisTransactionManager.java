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

import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils.RedisConnectionHolder;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.InvalidTimeoutException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.ResourceTransactionManager;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.transaction.PlatformTransactionManager} for a single {@link RedisConnectionFactory},
 * backed by Redis {@literal MULTI}/{@literal EXEC}/{@literal DISCARD}.
 * <p>
 * Only {@link RedisTemplate templates} with {@link RedisTemplate#setEnableTransactionSupport(boolean) transaction
 * support} enabled join the transaction. Other templates run their commands immediately.
 * <p>
 * Within a read-write transaction, write commands are queued and applied on commit. Their return value is
 * {@literal null}. Read commands run immediately on a separate connection and don't see queued writes. Read-only
 * transactions run all commands immediately.
 * <p>
 * Redis can't roll back after {@literal EXEC}: if a queued command fails, the other commands stay applied and commit
 * throws {@link TransactionSystemException}.
 * <p>
 * Isolation levels, timeouts, savepoints ({@link TransactionDefinition#PROPAGATION_NESTED}) and Redis Cluster are not
 * supported. A {@link #setDefaultTimeout(int) default timeout} is ignored.
 * <p>
 * {@link #setValidateExistingTransaction(boolean) Validates existing transactions} by default: a read-write scope can't
 * join a read-only transaction, because its writes would run immediately and couldn't be rolled back.
 *
 * @author Moritz Halbritter
 * @since 4.2
 * @see RedisTemplate#setEnableTransactionSupport(boolean)
 */
public class RedisTransactionManager extends AbstractPlatformTransactionManager
		implements ResourceTransactionManager {

	private final RedisConnectionFactory connectionFactory;

	/**
	 * Create a new {@link RedisTransactionManager}.
	 *
	 * @param connectionFactory the connection factory to manage transactions for.
	 */
	public RedisTransactionManager(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "RedisConnectionFactory must not be null");

		this.connectionFactory = connectionFactory;

		// Redis can't reject writes in a read-only transaction, so reject the read-write scope instead.
		setValidateExistingTransaction(true);
	}

	/**
	 * Return the {@link RedisConnectionFactory} this transaction manager manages transactions for.
	 *
	 * @return the connection factory.
	 */
	public RedisConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	@Override
	public Object getResourceFactory() {
		return this.connectionFactory;
	}

	@Override
	protected Object doGetTransaction() {
		return new RedisTransactionObject(RedisConnectionUtils.getManagedHolder(this.connectionFactory));
	}

	@Override
	protected boolean isExistingTransaction(Object transaction) {

		RedisConnectionHolder holder = ((RedisTransactionObject) transaction).getHolder();

		return holder != null && holder.isTransactionActive();
	}

	@Override
	protected void doBegin(Object transaction, TransactionDefinition definition) {

		validate(definition);

		RedisConnection connection = openConnection();
		RedisConnection connectionToUse = connection;

		// Read-write: queue writes via MULTI, route reads to a separate connection.
		try {
			if (!definition.isReadOnly()) {
				connectionToUse = RedisConnectionUtils.createConnectionSplittingProxy(connection, this.connectionFactory);
				connectionToUse.multi();
			}
		} catch (RuntimeException ex) {
			RedisConnectionUtils.doCloseConnection(connection);
			throw new CannotCreateTransactionException("Could not start Redis transaction", ex);
		}

		RedisConnectionHolder holder = new RedisConnectionHolder(connectionToUse);
		holder.setTransactionActive(true);
		holder.setSynchronizedWithTransaction(true);

		((RedisTransactionObject) transaction).setHolder(holder);
		RedisConnectionUtils.bindManagedHolder(this.connectionFactory, holder);
	}

	@Override
	protected void doCommit(DefaultTransactionStatus status) {

		RedisConnection connection = getConnection(status);

		if (status.isReadOnly()) {
			rejectPendingMulti(connection);
			return;
		}

		List<@Nullable Object> results;

		try {
			results = connection.exec();
		} catch (DataAccessException ex) {
			throw new TransactionSystemException("Could not commit Redis transaction", ex);
		}

		// Lettuce returns null for an empty transaction.
		if (results == null) {
			return;
		}

		// Unconverted results carry command errors as list entries instead of throwing.
		for (Object result : results) {
			if (result instanceof Exception ex) {
				throw new TransactionSystemException("Redis transaction command failed", ex);
			}
		}
	}

	@Override
	protected void doRollback(DefaultTransactionStatus status) {

		RedisConnection connection = getConnection(status);

		// Not queueing: read-only, or MULTI already ended by a (failed) EXEC.
		if (!connection.isQueueing()) {
			return;
		}

		discard(connection);
	}

	@Override
	protected void doSetRollbackOnly(DefaultTransactionStatus status) {
		((RedisTransactionObject) status.getTransaction()).getRequiredHolder().setRollbackOnly();
	}

	@Override
	protected Object doSuspend(Object transaction) {

		((RedisTransactionObject) transaction).setHolder(null);

		return RedisConnectionUtils.unbindManagedHolder(this.connectionFactory);
	}

	@Override
	protected void doResume(@Nullable Object transaction, Object suspendedResources) {
		RedisConnectionUtils.bindManagedHolder(this.connectionFactory, (RedisConnectionHolder) suspendedResources);
	}

	@Override
	protected void doCleanupAfterCompletion(Object transaction) {

		RedisConnectionUtils.unbindManagedHolder(this.connectionFactory);

		RedisConnectionHolder holder = ((RedisTransactionObject) transaction).getRequiredHolder();
		RedisConnectionUtils.doCloseConnection(holder.getConnection());
		holder.reset();
	}

	/**
	 * A read-only transaction never sends {@literal EXEC}, so commands queued after a manual {@code multi()} would be
	 * dropped silently. Discard them and fail instead.
	 */
	private static void rejectPendingMulti(RedisConnection connection) {

		if (!connection.isQueueing()) {
			return;
		}

		discard(connection);

		throw new IllegalTransactionStateException(
				"MULTI without EXEC in read-only Redis transaction; queued commands discarded");
	}

	private static void discard(RedisConnection connection) {

		try {
			connection.discard();
		} catch (DataAccessException ex) {
			throw new TransactionSystemException("Could not roll back Redis transaction", ex);
		}
	}

	private void validate(TransactionDefinition definition) {

		if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
			throw new InvalidIsolationLevelException("Redis does not support isolation levels");
		}

		// Ignore the default timeout: it may be applied to all transaction managers.
		int timeout = definition.getTimeout();

		if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
			throw new InvalidTimeoutException("Redis does not support transaction timeouts", timeout);
		}
	}

	private RedisConnection openConnection() {

		RedisConnection connection;

		try {
			connection = this.connectionFactory.getConnection();
		} catch (RuntimeException ex) {
			throw new CannotCreateTransactionException("Could not open Redis connection for transaction", ex);
		}

		// Cluster connections can't run MULTI.
		if (connection instanceof RedisClusterConnection) {
			RedisConnectionUtils.doCloseConnection(connection);
			throw new CannotCreateTransactionException("Redis Cluster does not support transactions");
		}

		return connection;
	}

	private static RedisConnection getConnection(DefaultTransactionStatus status) {
		return ((RedisTransactionObject) status.getTransaction()).getRequiredHolder().getRequiredConnection();
	}

	/**
	 * Transaction object holding the {@link RedisConnectionHolder}. Exposes rollback-only to the transaction
	 * infrastructure, so a participating scope can mark the whole transaction for rollback.
	 */
	private static class RedisTransactionObject implements SmartTransactionObject {

		private @Nullable RedisConnectionHolder holder;

		RedisTransactionObject(@Nullable RedisConnectionHolder holder) {
			this.holder = holder;
		}

		@Nullable RedisConnectionHolder getHolder() {
			return this.holder;
		}

		RedisConnectionHolder getRequiredHolder() {

			Assert.state(this.holder != null, "No RedisConnectionHolder available");

			return this.holder;
		}

		void setHolder(@Nullable RedisConnectionHolder holder) {
			this.holder = holder;
		}

		@Override
		public boolean isRollbackOnly() {
			return this.holder != null && this.holder.isRollbackOnly();
		}

		@Override
		public void flush() {
			TransactionSynchronizationUtils.triggerFlush();
		}

	}

}
