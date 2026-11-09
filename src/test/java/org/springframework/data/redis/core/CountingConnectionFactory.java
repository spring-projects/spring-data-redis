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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.util.ClassUtils;

/**
 * {@link RedisConnectionFactory} decorator counting connections that were obtained but not closed yet.
 *
 * @author Moritz Halbritter
 */
class CountingConnectionFactory implements RedisConnectionFactory {

	private final RedisConnectionFactory delegate;

	private final AtomicInteger active = new AtomicInteger();

	CountingConnectionFactory(RedisConnectionFactory delegate) {
		this.delegate = delegate;
	}

	/**
	 * Return the number of obtained connections that weren't closed yet.
	 */
	int getActiveConnections() {
		return this.active.get();
	}

	@Override
	public RedisConnection getConnection() {

		RedisConnection connection = this.delegate.getConnection();
		this.active.incrementAndGet();

		return count(connection);
	}

	@Override
	public RedisClusterConnection getClusterConnection() {
		return this.delegate.getClusterConnection();
	}

	@Override
	public boolean getConvertPipelineAndTxResults() {
		return this.delegate.getConvertPipelineAndTxResults();
	}

	@Override
	public RedisSentinelConnection getSentinelConnection() {
		return this.delegate.getSentinelConnection();
	}

	@Override
	public @Nullable DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return this.delegate.translateExceptionIfPossible(ex);
	}

	// Proxy with all interfaces of the target, so e.g. cluster detection still works.
	private RedisConnection count(RedisConnection connection) {

		AtomicBoolean closed = new AtomicBoolean();
		Class<?>[] interfaces = ClassUtils.getAllInterfaces(connection);

		return (RedisConnection) Proxy.newProxyInstance(getClass().getClassLoader(), interfaces, (proxy, method, args) -> {

			// Identity semantics, so held and passed-in proxies compare equal.
			if (method.getName().equals("equals")) {
				return proxy == args[0];
			}

			if (method.getName().equals("hashCode")) {
				return System.identityHashCode(proxy);
			}

			if (method.getName().equals("close") && closed.compareAndSet(false, true)) {
				this.active.decrementAndGet();
			}

			try {
				return method.invoke(connection, args);
			} catch (InvocationTargetException ex) {
				throw ex.getCause();
			}
		});
	}

}
