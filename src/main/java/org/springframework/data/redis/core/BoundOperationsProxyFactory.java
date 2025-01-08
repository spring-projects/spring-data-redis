/*
 * Copyright 2022-2025 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.data.projection.DefaultMethodInvokingMethodInterceptor;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

/**
 * Utility to create implementation objects for {@code Bound…Operations} so that bound key interfaces can be implemented
 * automatically by translating interface calls to actual {@code …Operations} interfaces.
 *
 * @author Mark Paluch
 * @author John Blum
 * @since 3.0
 */
class BoundOperationsProxyFactory {

	private final Map<Method, Method> targetMethodCache = new ConcurrentHashMap<>();

	/**
	 * Create a proxy object that implements {@link Class boundOperationsInterface} using the given {@code key} and
	 * {@link DataType}. Calls to {@code Bound…Operations} methods are bridged by forwarding these either to the
	 * {@code operationsTarget} or a default implementation.
	 *
	 * @param boundOperationsInterface the {@code Bound…Operations} interface.
	 * @param key the bound key.
	 * @param type the {@link DataType} for which to create a proxy object.
	 * @param operations the {@link RedisOperations} instance.
	 * @param operationsTargetFunction function to extract the actual delegate for method calls.
	 * @return the proxy object.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> T createProxy(Class<T> boundOperationsInterface, Object key, DataType type,
			RedisOperations<?, ?> operations, Function<RedisOperations<?, ?>, Object> operationsTargetFunction) {

		DefaultBoundKeyOperations delegate = new DefaultBoundKeyOperations(type, key, (RedisOperations) operations);
		Object operationsTarget = operationsTargetFunction.apply(operations);

		ProxyFactory proxyFactory = new ProxyFactory();
		proxyFactory.addInterface(boundOperationsInterface);
		proxyFactory.addAdvice(new DefaultMethodInvokingMethodInterceptor());
		proxyFactory.addAdvice(
				new BoundOperationsMethodInterceptor(key, operations, boundOperationsInterface, operationsTarget, delegate));

		return (T) proxyFactory.getProxy(getClass().getClassLoader());
	}

	Method lookupRequiredMethod(Method method, Class<?> targetClass, boolean considerKeyArgument) {

		Method target = lookupMethod(method, targetClass, considerKeyArgument);

		if (target == null) {
			throw new IllegalArgumentException("Cannot lookup target method for %s in class %s; This appears to be a bug"
					.formatted(method, targetClass.getName()));
		}

		return target;
	}

	@Nullable
	Method lookupMethod(Method method, Class<?> targetClass, boolean considerKeyArgument) {

		return targetMethodCache.computeIfAbsent(method, it -> {

			Class<?>[] paramTypes;

			if (isStreamRead(method)) {
				paramTypes = new Class[it.getParameterCount()];
				System.arraycopy(it.getParameterTypes(), 0, paramTypes, 0, paramTypes.length - 1);
				paramTypes[paramTypes.length - 1] = StreamOffset[].class;
			} else if (considerKeyArgument) {

				paramTypes = new Class[it.getParameterCount() + 1];
				paramTypes[0] = Object.class;
				System.arraycopy(it.getParameterTypes(), 0, paramTypes, 1, paramTypes.length - 1);
			} else {
				paramTypes = it.getParameterTypes();
			}

			return ReflectionUtils.findMethod(targetClass, method.getName(), paramTypes);
		});
	}

	private boolean isStreamRead(Method method) {
		return method.getName().equals("read")
				&& method.getParameterTypes()[method.getParameterCount() - 1].equals(ReadOffset.class);
	}

	/**
	 * {@link MethodInterceptor} to delegate proxy calls to either {@link RedisOperations}, {@code key},
	 * {@link DefaultBoundKeyOperations} or the {@code operationsTarget} such as {@link ValueOperations}.
	 */
	class BoundOperationsMethodInterceptor implements MethodInterceptor {

		private final Class<?> boundOperationsInterface;
		private final Object operationsTarget;
		private final DefaultBoundKeyOperations delegate;

		public BoundOperationsMethodInterceptor(Object key, RedisOperations<?, ?> operations,
				Class<?> boundOperationsInterface, Object operationsTarget, DefaultBoundKeyOperations delegate) {

			this.boundOperationsInterface = boundOperationsInterface;
			this.operationsTarget = operationsTarget;
			this.delegate = delegate;
		}

		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {

			Method method = invocation.getMethod();

			return switch (method.getName()) {
				case "getKey" -> delegate.getKey();
				case "rename" -> {
					delegate.rename(invocation.getArguments()[0]);
					yield null;
				}
				case "getOperations" -> delegate.getOps();
				default ->
					method.getDeclaringClass() == boundOperationsInterface ? doInvoke(invocation, method, operationsTarget, true)
							: doInvoke(invocation, method, delegate, false);
			};
		}

		@Nullable
		private Object doInvoke(MethodInvocation invocation, Method method, Object target, boolean considerKeyArgument) {

			Method backingMethod = lookupRequiredMethod(method, target.getClass(), considerKeyArgument);

			Object[] args;
			Object[] invocationArguments = invocation.getArguments();

			if (isStreamRead(method)) {
				// stream.read requires translation to StreamOffset using the bound key.
				args = new Object[backingMethod.getParameterCount()];
				System.arraycopy(invocationArguments, 0, args, 0, args.length - 1);
				args[args.length - 1] = new StreamOffset[] {
						StreamOffset.create(delegate.getKey(), (ReadOffset) invocationArguments[invocationArguments.length - 1]) };
			} else if (backingMethod.getParameterCount() > 0 && backingMethod.getParameterTypes()[0].equals(Object.class)) {

				args = new Object[backingMethod.getParameterCount()];
				args[0] = delegate.getKey();
				System.arraycopy(invocationArguments, 0, args, 1, args.length - 1);
			} else {
				args = invocationArguments;
			}

			try {
				return backingMethod.invoke(target, args);
			} catch (ReflectiveOperationException ex) {
				ReflectionUtils.handleReflectionException(ex);
				throw new UnsupportedOperationException("Should not happen", ex);
			}
		}
	}

	/**
	 * Default {@link BoundKeyOperations} implementation. Meant for internal usage.
	 *
	 * @author Costin Leau
	 * @author Christoph Strobl
	 */
	static class DefaultBoundKeyOperations implements BoundKeyOperations<Object> {

		private final DataType type;
		private Object key;
		private final RedisOperations<Object, ?> ops;

		DefaultBoundKeyOperations(DataType type, Object key, RedisOperations<Object, ?> operations) {
			this.type = type;

			this.key = key;
			this.ops = operations;
		}

		@Override
		public Object getKey() {
			return key;
		}

		@Override
		public Boolean expire(long timeout, TimeUnit unit) {
			return ops.expire(key, timeout, unit);
		}

		@Override
		public Boolean expireAt(Date date) {
			return ops.expireAt(key, date);
		}

		@Override
		public Long getExpire() {
			return ops.getExpire(key);
		}

		@Override
		public Boolean persist() {
			return ops.persist(key);
		}

		@Override
		public void rename(Object newKey) {
			if (ops.hasKey(key)) {
				ops.rename(key, newKey);
			}
			key = newKey;
		}

		public DataType getType() {
			return type;
		}

		public RedisOperations<Object, ?> getOps() {
			return ops;
		}
	}
}
