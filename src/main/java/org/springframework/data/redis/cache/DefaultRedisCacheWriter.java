/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.redis.cache;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

/**
 * {@link RedisCacheWriter} implementation capable of reading/writing binary data from/to Redis in {@literal standalone}
 * and {@literal cluster} environments. Works upon a given {@link RedisConnectionFactory} to obtain the actual
 * {@link RedisConnection}. <br />
 * {@link DefaultRedisCacheWriter} can be used in {@link #lockingRedisCacheWriter(RedisConnectionFactory) locking} or
 * {@link #nonLockingRedisCacheWriter(RedisConnectionFactory) non-locking} mode. While {@literal non-locking} aims for
 * maximum performance it may result in overlapping, non atomic, command execution for operations spanning multiple
 * Redis interactions like {@code putIfAbsent}. The {@literal locking} counterpart prevents command overlap by setting
 * an explicit lock key and checking against presence of this key which leads to additional requests and potential
 * command wait times.
 *
 * @author Christoph Strobl
 * @since 2.0
 */
public class DefaultRedisCacheWriter implements RedisCacheWriter {

	private static final byte[] CLEAN_SCRIPT = "local keys = redis.call('KEYS', ARGV[1]); local keysCount = table.getn(keys); if(keysCount > 0) then for _, key in ipairs(keys) do redis.call('del', key); end; end; return keysCount;"
			.getBytes(Charset.forName("UTF-8"));

	private final RedisConnectionFactory connectionFactory;
	private final Duration sleepTime;

	/**
	 * @param connectionFactory must not be {@literal null}.
	 */
	DefaultRedisCacheWriter(RedisConnectionFactory connectionFactory) {
		this(connectionFactory, Duration.ZERO);
	}

	/**
	 * @param connectionFactory must not be {@literal null}.
	 * @param sleepTime sleep time between lock request attempts. Must not be {@literal null}. Use {@link Duration#ZERO}
	 *          to disable locking.
	 */
	DefaultRedisCacheWriter(RedisConnectionFactory connectionFactory, Duration sleepTime) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");
		Assert.notNull(sleepTime, "SleepTime must not be null!");

		this.connectionFactory = connectionFactory;
		this.sleepTime = sleepTime;
	}

	/**
	 * Create new {@link RedisCacheWriter} without locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 */
	public static DefaultRedisCacheWriter nonLockingRedisCacheWriter(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");
		return new DefaultRedisCacheWriter(connectionFactory);
	}

	/**
	 * Create new {@link RedisCacheWriter} with locking behavior.
	 *
	 * @param connectionFactory must not be {@literal null}.
	 * @return new instance of {@link DefaultRedisCacheWriter}.
	 */
	public static DefaultRedisCacheWriter lockingRedisCacheWriter(RedisConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");
		return new DefaultRedisCacheWriter(connectionFactory, Duration.ofMillis(50));
	}

	@Override
	public void put(String name, byte[] key, byte[] value, Duration ttl) {

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");
		Assert.notNull(ttl, "Ttl must not be null!");

		execute(name, connection -> {

			if (shouldExpireWithin(ttl)) {
				connection.set(key, value, Expiration.from(ttl.toMillis(), TimeUnit.MILLISECONDS), SetOption.upsert());
			} else {
				connection.set(key, value);
			}

			return "OK";
		});
	}

	@Override
	public byte[] get(String name, byte[] key) {

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(key, "Key must not be null!");

		return execute(name, connection -> connection.get(key));
	}

	@Override
	public byte[] putIfAbsent(String name, byte[] key, byte[] value, Duration ttl) {

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(key, "Key must not be null!");
		Assert.notNull(value, "Value must not be null!");
		Assert.notNull(ttl, "Ttl must not be null!");

		return execute(name, connection -> {

			if (isLockingCacheWriter()) {
				doLock(name, connection);
			}

			try {
				if (connection.setNX(key, value)) {

					if (shouldExpireWithin(ttl)) {
						connection.pExpire(key, ttl.toMillis());
					}
					return null;
				}

				return connection.get(key);
			} finally {

				if (isLockingCacheWriter()) {
					doUnlock(name, connection);
				}
			}
		});
	}

	@Override
	public void remove(String name, byte[] key) {

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(key, "Key must not be null!");

		execute(name, connection -> connection.del(key));
	}

	/**
	 * Explicitly set a write lock on a cache.
	 *
	 * @param name the name of the cache to lock.
	 */
	public void lock(String name) {
		execute(name, connection -> doLock(name, connection));
	}

	private Boolean doLock(String name, RedisConnection connection) {
		return connection.setNX(createCacheLockKey(name), new byte[] {});
	}

	/**
	 * Explicitly remove a write lock from a cache.
	 *
	 * @param name the name of the cache to unlock.
	 */
	public void unlock(String name) {
		executeWithoutLockCheck(connection -> doUnlock(name, connection));
	}

	private Long doUnlock(String name, RedisConnection connection) {
		return connection.del(createCacheLockKey(name));
	}

	/**
	 * Check if a cache has set a lock.
	 *
	 * @param name the name of the cache to check for presence of a lock.
	 * @return {@literal true} if lock found.
	 */
	public boolean isLoked(String name) {
		return executeWithoutLockCheck(connection -> doCheckLock(name, connection));
	}

	private boolean doCheckLock(String name, RedisConnection connection) {
		return connection.exists(createCacheLockKey(name));
	}

	@Override
	public void clean(String name, byte[] pattern) {

		Assert.notNull(name, "Name must not be null!");
		Assert.notNull(pattern, "Pattern must not be null!");

		execute(name, connection -> {

			boolean wasLocked = false;

			try {
				if (connection instanceof RedisClusterConnection) {

					if (isLockingCacheWriter()) {
						doLock(name, connection);
						wasLocked = true;
					}

					byte[][] keys = connection.keys(pattern).stream().toArray(size -> new byte[size][]);
					connection.del(keys);
				} else {
					connection.eval(CLEAN_SCRIPT, ReturnType.INTEGER, 0, pattern);
				}
			} finally {

				if (wasLocked && isLockingCacheWriter()) {
					doUnlock(name, connection);
				}
			}

			return "OK";
		});
	}

	/**
	 * @return {@literal true} if {@link RedisCacheWriter} uses locks.
	 */
	public boolean isLockingCacheWriter() {
		return !sleepTime.isZero() && !sleepTime.isNegative();
	}

	<T> T execute(String name, ConnectionCallback<T> callback) {

		RedisConnection connection = connectionFactory.getConnection();
		try {

			checkAndPotentiallyWaitUntilUnlocked(name, connection);
			return callback.doWithConnection(connection);
		} finally {
			connection.close();
		}
	}

	private <T> T executeWithoutLockCheck(ConnectionCallback<T> callback) {

		RedisConnection connection = connectionFactory.getConnection();

		try {
			return callback.doWithConnection(connection);
		} finally {
			connection.close();
		}
	}

	private void checkAndPotentiallyWaitUntilUnlocked(String name, RedisConnection connection) {

		if (isLockingCacheWriter()) {

			long timeout = sleepTime.toMillis();

			while (doCheckLock(name, connection)) {
				try {
					Thread.sleep(timeout);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private boolean shouldExpireWithin(Duration ttl) {
		return ttl != null && !ttl.isZero() && !ttl.isNegative();
	}

	byte[] createCacheLockKey(String name) {
		return (name + "~lock").getBytes(Charset.forName("UTF-8"));
	}

	/**
	 * @param <T>
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	interface ConnectionCallback<T> {

		T doWithConnection(RedisConnection connection);
	}
}
