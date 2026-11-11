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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.jspecify.annotations.Nullable;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisConnectionUtils.RedisConnectionProxy;
import org.springframework.util.ClassUtils;

/**
 * {@link InvocationHandler} for a {@link RedisConnection} that is bound to the current thread and handed out as sticky
 * connection. Closing the connection, or a {@link Cursor} obtained from it, releases the reference to the bound
 * connection through {@link RedisConnectionUtils#releaseConnection(RedisConnection, RedisConnectionFactory)} instead
 * of closing the bound connection itself.
 *
 * @author Sparshal Kothari
 * @since 4.2
 * @see RedisTemplate#executeWithStickyConnection(RedisCallback)
 */
class BoundConnectionReleasingInvocationHandler implements InvocationHandler {

	private final RedisConnection target;
	private final RedisConnectionFactory factory;
	private final AtomicBoolean released = new AtomicBoolean();

	private BoundConnectionReleasingInvocationHandler(RedisConnection target, RedisConnectionFactory factory) {
		this.target = target;
		this.factory = factory;
	}

	/**
	 * Create a proxy for the given bound {@link RedisConnection} that releases the connection instead of closing it.
	 *
	 * @param target the connection bound to the current thread.
	 * @param factory the factory the connection is bound for.
	 * @return the proxied connection.
	 */
	static RedisConnection createProxy(RedisConnection target, RedisConnectionFactory factory) {

		Set<Class<?>> interfaces = new LinkedHashSet<>();
		Collections.addAll(interfaces,
				ClassUtils.getAllInterfacesForClass(target.getClass(), RedisConnectionProxy.class.getClassLoader()));
		interfaces.add(RedisConnectionProxy.class);

		return (RedisConnection) Proxy.newProxyInstance(RedisConnectionProxy.class.getClassLoader(),
				interfaces.toArray(new Class<?>[0]), new BoundConnectionReleasingInvocationHandler(target, factory));
	}

	@Override
	public @Nullable Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {

		switch (method.getName()) {
			case "equals" -> {
				return args != null && proxy == args[0];
			}
			case "hashCode" -> {
				return System.identityHashCode(proxy);
			}
			case "getTargetConnection" -> {
				return target;
			}
			case "close" -> {
				release();
				return null;
			}
			case "isClosed" -> {
				return released.get() || target.isClosed();
			}
		}

		Object result;
		try {
			result = method.invoke(target, args);
		} catch (InvocationTargetException ex) {
			throw ex.getTargetException();
		}

		return result instanceof Cursor<?> cursor ? new ReleasingCursor<>(cursor) : result;
	}

	private void release() {

		if (released.compareAndSet(false, true)) {
			RedisConnectionUtils.releaseConnection(target, factory);
		}
	}

	/**
	 * {@link Cursor} that releases the bound connection on {@link #close()} instead of closing the connection through the
	 * delegate.
	 */
	private class ReleasingCursor<T> implements Cursor<T> {

		private final Cursor<T> delegate;
		private boolean closed;

		ReleasingCursor(Cursor<T> delegate) {
			this.delegate = delegate;
		}

		@Override
		public boolean hasNext() {

			assertCursorIsOpen();
			return delegate.hasNext();
		}

		@Override
		public T next() {

			assertCursorIsOpen();
			return delegate.next();
		}

		@Override
		public void remove() {
			delegate.remove();
		}

		@Override
		public CursorId getId() {
			return delegate.getId();
		}

		@Override
		@Deprecated
		public long getCursorId() {
			return delegate.getCursorId();
		}

		@Override
		public long getPosition() {
			return delegate.getPosition();
		}

		@Override
		public boolean isClosed() {
			return closed || delegate.isClosed();
		}

		@Override
		public void close() {

			closed = true;
			release();
		}

		private void assertCursorIsOpen() {

			if (closed) {
				throw new InvalidDataAccessApiUsageException("Cannot access closed cursor");
			}
		}
	}
}
