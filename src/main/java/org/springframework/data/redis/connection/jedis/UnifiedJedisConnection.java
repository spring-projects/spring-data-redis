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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.UnifiedJedis;

import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.util.Assert;

/**
 * {@link RedisConnection} implementation that uses a pooled {@link RedisClient} instance.
 * <p>
 * This connection extends {@link JedisConnection} and uses a shared {@link RedisClient} instance
 * that manages its own internal connection pool. Unlike the traditional {@link JedisConnection},
 * closing this connection does not close the underlying pool - it simply marks the connection
 * as closed while the pool continues to be managed by the factory.
 * <p>
 * <b>Unsupported operations:</b> Some operations are not supported with pooled connections because
 * they would affect shared pool state:
 * <ul>
 *   <li>{@link #select(int)} - would change database on a shared connection</li>
 *   <li>{@link #setClientName(byte[])} - would rename connections in the pool</li>
 * </ul>
 * Configure these settings via {@link JedisConnectionFactory} instead.
 * <p>
 * <b>Transaction handling:</b> When using {@link RedisClient} with internal connection pooling,
 * WATCH commands require special handling. Since each command could potentially execute on a
 * different connection from the pool, calling {@link #watch(byte[]...)} binds to a specific
 * connection by starting a transaction with {@code doMulti=false}. This ensures WATCH, MULTI,
 * and EXEC all execute on the same connection. The MULTI command is sent when {@link #multi()}
 * is called.
 *
 * @author Tihomir Mateev
 * @since 4.1
 * @see JedisConnection
 * @see RedisClient
 */
@NullUnmarked
class UnifiedJedisConnection extends JedisConnection {

	private volatile boolean closed = false;

	private final UnifiedJedis unifiedJedis;

	private boolean isMultiExecuted = false;

	/**
	 * Constructs a new {@link UnifiedJedisConnection} using a pooled {@link UnifiedJedis}.
	 *
	 * @param jedis the pooled {@link UnifiedJedis} instance (typically a {@link RedisClient})
	 * @throws IllegalArgumentException if jedis is {@literal null}
	 */
	 UnifiedJedisConnection(@NonNull UnifiedJedis jedis) {
		super(jedis);
		Assert.notNull(jedis, "UnifiedJedis must not be null");
		this.unifiedJedis = jedis;
	}

	@Override
	protected void doClose() {
		// Clean up any open pipeline to return connection to the pool
		AbstractPipeline currentPipeline = getPipeline();
		if (currentPipeline != null) {
			try {
				currentPipeline.close();
			} catch (Exception ignored) {
				// Ignore errors during cleanup
			}
			this.pipeline = null;
		}

		// Clean up any open transaction to return connection to the pool
		AbstractTransaction currentTransaction = getTransaction();
		if (currentTransaction != null) {
			try {
				// Try to discard first to cleanly end the transaction
				currentTransaction.discard();
			} catch (Exception ignored) {
				// Transaction might not be in a state that allows discard
			}
			try {
				currentTransaction.close();
			} catch (Exception ignored) {
				// Ignore errors during cleanup
			}
			this.transaction = null;
			this.isMultiExecuted = false;
		}

		this.closed = true;
		// Do NOT close the instance - it manages the pool internally and should only be closed when the factory is destroyed
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public Object getNativeConnection() {
		return unifiedJedis;
	}

	@Override
	@NonNull
	public UnifiedJedis getJedis() {
		return this.unifiedJedis;
	}

	/**
	 * Not supported with pooled connections. Configure the database via {@link JedisConnectionFactory} instead.
	 *
	 * @param dbIndex the database index (ignored)
	 * @throws InvalidDataAccessApiUsageException always
	 */
	@Override
	public void select(int dbIndex) {
		throw new InvalidDataAccessApiUsageException(
				"SELECT is not supported with pooled connections. Configure the database in the connection factory instead.");
	}

	/**
	 * Not supported with pooled connections. Configure the client name via
	 * {@link JedisConnectionFactory#setClientName(String)} instead.
	 *
	 * @param name the client name (ignored)
	 * @throws InvalidDataAccessApiUsageException always
	 */
	@Override
	public void setClientName(byte @NonNull [] name) {
		throw new InvalidDataAccessApiUsageException(
				"setClientName is not supported with pooled connections. " +
						"Configure the client name via JedisConnectionFactory.setClientName() or JedisClientConfig instead.");
	}

	/**
	 * Watches the given keys for modifications during a transaction. Binds to a dedicated
	 * connection from the pool to ensure WATCH, MULTI, and EXEC execute on the same connection.
	 *
	 * @param keys the keys to watch
	 * @throws InvalidDataAccessApiUsageException if called while a transaction is active
	 */
	@Override
	public void watch(byte @NonNull [] @NonNull... keys) {

		if (isMultiExecuted()) {
			throw new InvalidDataAccessApiUsageException("WATCH is not supported when a transaction is active");
		} else if(!isQueueing()) {
			this.transaction = getJedis().transaction(false);
		}

		this.transaction.watch(keys);
	}

	/**
	 * Unwatches all previously watched keys. Releases the dedicated connection back to the pool
	 * if MULTI was not yet called.
	 */
	@Override
	public void unwatch() {
		AbstractTransaction tx = getTransaction();
		if (tx != null) {
			try {
				tx.unwatch();
			} finally {
				// Only close if MULTI was not yet executed (still in WATCH-only state)
				if (!this.isMultiExecuted) {
					try {
						tx.close();
					} catch (Exception ignored) {
						// Ignore errors during close
					}
					this.transaction = null;
				}
			}
		}
	}

	/**
	 * Starts a Redis transaction. If WATCH was called previously, sends MULTI on the same
	 * dedicated connection. Otherwise, creates a new transaction.
	 *
	 * @throws InvalidDataAccessApiUsageException if a pipeline is open
	 */
	@Override
	public void multi() {

		if (isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Transaction while a pipeline is open");
		}

		if (!isMultiExecuted()) {
			if (isQueueing()) {
				// watch was called previously and a transaction is already in progress
				this.transaction.multi();
				this.isMultiExecuted = true;
			} else {
				// pristine connection, start a new transaction
				this.transaction = unifiedJedis.multi();
				this.isMultiExecuted = true;
			}
		}
	}

	/**
	 * Executes all queued commands in the transaction and returns the connection to the pool.
	 *
	 * @return list of command results, or {@literal null} if the transaction was aborted
	 * @throws InvalidDataAccessApiUsageException if no transaction is active
	 */
	@Override
	public List<@Nullable Object> exec() {
		AbstractTransaction tx = getTransaction();
		try {
			return super.exec();
		} finally {
			this.isMultiExecuted = false;
			if (tx != null) {
				try {
					tx.close();
				} catch (Exception ignored) {
				}
			}
		}
	}

	/**
	 * Discards all queued commands and returns the connection to the pool.
	 *
	 * @throws InvalidDataAccessApiUsageException if no transaction is active
	 */
	@Override
	public void discard() {
		AbstractTransaction tx = getTransaction();
		try {
			super.discard();
		} finally {
			this.isMultiExecuted = false;
			if (tx != null) {
				try {
					tx.close();
				} catch (Exception ignored) {
				}
			}
		}
	}

	/**
	 * Closes the pipeline and returns the connection to the pool.
	 *
	 * @return list of pipeline command results
	 */
	@Override
	public List<@Nullable Object> closePipeline() {
		AbstractPipeline currentPipeline = getPipeline();
		if (currentPipeline != null) {
			try {
				// First sync and convert results (parent logic)
				List<@Nullable Object> results = super.closePipeline();
				return results;
			} finally {
				// Close the pipeline to return the connection to the pool
				// This must happen even if sync/conversion fails
				try {
					currentPipeline.close();
				} catch (Exception ignored) {
					// Ignore errors during close - connection may already be closed
				}
			}
		}
		return Collections.emptyList();
	}

	private boolean isMultiExecuted(){
		return isQueueing() && this.isMultiExecuted;
	}


}

