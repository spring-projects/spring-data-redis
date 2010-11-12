/*
 * Copyright 2010 the original author or authors.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.springframework.datastore.redis.connection.RedisConnection;
import org.springframework.datastore.redis.connection.RedisConnectionFactory;
import org.springframework.datastore.redis.serializer.RedisSerializer;
import org.springframework.datastore.redis.serializer.SimpleRedisSerializer;
import org.springframework.datastore.redis.serializer.StringRedisSerializer;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * 
 * Helper class that simplifies Redis data access code. Automatically converts Redis connection exceptions into 
 * DataAccessExceptions, following the org.springframework.dao exception hierarchy.
 *
 * The central method is execute, supporting Redis access code implementing the {@link RedisCallback} interface.
 * It provides {@link RedisConnection} handling such that neither the {@link RedisCallback} implementation nor 
 * the calling code needs to explicitly care about retrieving/closing Redis connections, or handling Session 
 * lifecycle exceptions. For typical single step actions, there are various convenience methods.
 *  
 * <b>This is the central class in Redis support</b>.
 * Simplifies the use of Redis and helps avoid common errors.
 * 
 * @author Costin Leau
 */
public class RedisTemplate<K, V> extends RedisAccessor implements RedisOperations<K, V> {

	private boolean exposeConnection = false;
	private RedisSerializer keySerializer = new StringRedisSerializer();
	private RedisSerializer valueSerializer = new SimpleRedisSerializer();
	private RedisSerializer defaultSerializer = new SimpleRedisSerializer();

	public RedisTemplate() {
	}

	public RedisTemplate(RedisConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	public void del(final String redisKey) {
		execute(new RedisCallback<Object>() {
			@Override
			public Object doInRedis(RedisConnection connection) throws Exception {
				connection.del(keySerializer.serialize(redisKey));
				return null;
			}
		});
	}


	public <T> T execute(RedisCallback<T> action) {
		return execute(action, isExposeConnection());
	}

	public <T> T execute(RedisCallback<T> action, boolean exposeConnection) {
		return execute(action, isExposeConnection(), defaultSerializer);
	}

	public <T> T execute(RedisCallback<T> action, boolean exposeConnection, RedisSerializer returnSerializer) {
		Assert.notNull(action, "Callback object must not be null");

		RedisConnectionFactory factory = getConnectionFactory();
		RedisConnection conn = RedisConnectionUtils.getRedisConnection(factory);

		boolean existingConnection = TransactionSynchronizationManager.hasResource(factory);

		try {
			RedisConnection connToExpose = (exposeConnection ? conn : createRedisConnectionProxy(conn));
			T result = action.doInRedis(connToExpose);
			// TODO: should do flush?
			return postProcessResult(result, conn, existingConnection);
		} catch (Exception ex) {
			// TODO: too generic ?
			throw tryToConvertRedisAccessException(ex);
		} finally {
			RedisConnectionUtils.releaseConnection(conn, factory);
		}
	}

	protected RedisConnection createRedisConnectionProxy(RedisConnection pm) {
		Class<?>[] ifcs = ClassUtils.getAllInterfacesForClass(pm.getClass(), getClass().getClassLoader());
		return (RedisConnection) Proxy.newProxyInstance(pm.getClass().getClassLoader(), ifcs,
				new CloseSuppressingInvocationHandler(pm));
	}

	protected <T> T postProcessResult(T result, RedisConnection conn, boolean existingConnection) {
		return result;
	}

	/**
	 * Returns the exposeConnection.
	 *
	 * @return Returns the exposeConnection
	 */
	public boolean isExposeConnection() {
		return exposeConnection;
	}

	/**
	 * Sets whether to expose the Redis connection to {@link RedisCallback} code.
	 * 
	 * Default is "false": a proxy will be returned, suppressing <tt>quit</tt> and <tt>disconnect</tt> calls.
	 *  
	 * @param exposeConnection
	 */
	public void setExposeConnection(boolean exposeConnection) {
		this.exposeConnection = exposeConnection;
	}

	public void setKeySerializer(RedisSerializer serializer) {
		this.keySerializer = serializer;
	}

	public void setValueSerializer(RedisSerializer serializer) {
		this.valueSerializer = serializer;
	}

	public void setDefaultSerializer(RedisSerializer serializer) {
		this.defaultSerializer = serializer;
	}

	/**
	 * Invocation handler that suppresses close calls on JDO PersistenceManagers.
	 * Also prepares returned Query objects.
	 * @see RedisConnection#close()
	 */
	private class CloseSuppressingInvocationHandler implements InvocationHandler {

		private final RedisConnection target;

		public CloseSuppressingInvocationHandler(RedisConnection target) {
			this.target = target;
		}

		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			// Invocation on PersistenceManager interface (or provider-specific extension) coming in...

			if (method.getName().equals("equals")) {
				// Only consider equal when proxies are identical.
				return (proxy == args[0]);
			}
			else if (method.getName().equals("hashCode")) {
				// Use hashCode of PersistenceManager proxy.
				return System.identityHashCode(proxy);
			}
			else if (method.getName().equals("close")) {
				// Handle close method: suppress, not valid.
				return null;
			}

			// Invoke method on target RedisConnection.
			try {
				Object retVal = method.invoke(this.target, args);
				return retVal;
			} catch (InvocationTargetException ex) {
				throw ex.getTargetException();
			}
		}
	}

	private byte[] rawKey(K key) {
		return (key != null ? keySerializer.serialize(key) : null);
	}

	private byte[] rawValue(V value) {
		return (value != null ? valueSerializer.serialize(value) : null);
	}

	// utility methods for the template internal methods
	private abstract class DeserializingRedisCallback implements RedisCallback<V> {
		private K key;

		public DeserializingRedisCallback(K key) {
			this.key = key;
		}

		@SuppressWarnings("unchecked")
		@Override
		public final V doInRedis(RedisConnection connection) throws Exception {
			byte[] result = inRedis(rawKey(key), connection);
			if (result != null) {
				return (V) valueSerializer.deserialize(result);
			}
			return null;
		}

		protected abstract byte[] inRedis(byte[] rawKey, RedisConnection connection);
	}


	//
	// RedisOperations
	//

	@Override
	public Object exec() {
		throw new UnsupportedOperationException();
	}

	@Override
	public BoundListOperations<K, V> forList(K key) {
		return new DefaultBoundListOperations<K, V>(key, this);
	}

	@Override
	public V get(final K key) {
		return execute(new DeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.get(rawKey);
			}

		}, false);
	}

	@Override
	public V getAndSet(K key, V newValue) {
		final byte[] rawValue = rawValue(newValue);
		return execute(new DeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				return connection.getSet(rawKey, rawValue);
			}

		}, false);
	}

	@Override
	public Integer increment(K key, final int delta) {
		final byte[] rawKey = rawKey(key);
		return execute(new RedisCallback<Integer>() {

			@Override
			public Integer doInRedis(RedisConnection connection) throws Exception {
				if (delta == 1) {
					return connection.incr(rawKey);
				}

				if (delta == -1) {
					return connection.decr(rawKey);
				}

				if (delta < 0) {
					return connection.decrBy(rawKey, delta);
				}

				return connection.incrBy(rawKey, delta);
			}
		}, false);
	}

	@Override
	public ListOperations<K, V> listOps() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void multi() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void set(K key, V value) {
		final byte[] rawValue = rawValue(value);
		execute(new DeserializingRedisCallback(key) {

			@Override
			protected byte[] inRedis(byte[] rawKey, RedisConnection connection) {
				connection.set(rawKey, rawValue);
				return null;
			}

		}, false);
	}

	@Override
	public void watch(K... keys) {
		final byte[][] rawKeys = new byte[keys.length][];

		for (int i = 0; i < keys.length; i++) {
			rawKeys[i] = rawKey(keys[i]);
		}

		execute(new RedisCallback<Object>() {

			@Override
			public Object doInRedis(RedisConnection connection) throws Exception {
				connection.watch(rawKeys);
				return null;
			}
		}, false);
	}
}