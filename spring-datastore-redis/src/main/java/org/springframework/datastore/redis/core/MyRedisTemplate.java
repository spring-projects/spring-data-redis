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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.springframework.datastore.redis.core.connection.RedisConnection;
import org.springframework.datastore.redis.core.connection.RedisConnectionFactory;
import org.springframework.datastore.redis.support.converter.RedisConverter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * 
 * Helper class that simplifies Redis data access code. Automatically converts Redis client exceptions into 
 * DataAccessExceptions, following the org.springframework.dao exception hierarchy.
 *
 * The central method is execute, supporting Redis access code implementing the {@link MyRedisCallback} interface.
 * It provides {@link RedisConnection} handling such that neither the {@link MyRedisCallback} implementation nor 
 * the calling code needs to explicitly care about retrieving/closing Redis connections, or handling Session 
 * lifecycle exceptions. For typical single step actions, there are various convenience methods.
 *  
 * <b>This is the central class in Redis support</b>.
 * Simplifies the use of Redis and helps avoid common errors.
 * 
 * @author Costin Leau
 */
public class MyRedisTemplate extends MyRedisAccessor {

	private boolean exposeConnection = false;
	private RedisConverter converter = null;

	public MyRedisTemplate() {
	}

	public MyRedisTemplate(RedisConnectionFactory connectionFactory) {
		this.setConnectionFactory(connectionFactory);
		afterPropertiesSet();
	}

	public <T> T execute(MyRedisCallback<T> action) {
		return execute(action, isExposeConnection());
	}


	public <T> T execute(MyRedisCallback<T> action, boolean exposeConnection) {
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
	 * Sets whether to expose the Redis connection to {@link MyRedisCallback} code.
	 * 
	 * Default is "false": a proxy will be returned, suppressing <tt>quit</tt> and <tt>disconnect</tt> calls.
	 *  
	 * @param exposeConnection
	 */
	public void setExposeConnection(boolean exposeConnection) {
		this.exposeConnection = exposeConnection;
	}

	public void setRedisConverter(RedisConverter converter) {
		this.converter = converter;
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
}