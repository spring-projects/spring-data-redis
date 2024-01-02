/*
 * Copyright 2020-2024 the original author or authors.
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
package org.springframework.data.redis.test.extension;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import org.springframework.core.ResolvableType;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.util.Lazy;

/**
 * JUnit 5 {@link Extension} using Jedis providing parameter resolution for connection resources and that reacts to
 * callbacks. The following resource types are supported by this extension:
 * <ul>
 * <li>{@link Jedis} (singleton)</li>
 * <li>{@link JediCluster} (singleton)</li>
 * </ul>
 *
 * <pre class="code">
 * &#064;ExtendWith(JedisExtension.class)
 * public class CustomCommandTest {
 *
 * 	private final Jedis jedis;
 *
 * 	public CustomCommandTest(Jedis jedis) {
 * 		this.jedis = jedis;
 * 	}
 *
 * }
 * </pre>
 *
 * <h3>Resource lifecycle</h3> This extension allocates resources lazily and stores them in its {@link ExtensionContext}
 * {@link ExtensionContext.Store} for reuse across multiple tests. Connections are allocated through suppliers. Shutdown
 * is managed by this extension.
 *
 * @author Mark Paluch
 * @see ParameterResolver
 * @see BeforeEachCallback
 */
public class JedisExtension implements ParameterResolver {

	private final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create(JedisExtension.class);

	private static final Set<Class<?>> SUPPORTED_INJECTABLE_TYPES = new HashSet<>(
			Arrays.asList(Jedis.class, JedisCluster.class));

	private static final List<Supplier<?>> SUPPLIERS = Arrays.asList(JedisSupplier.INSTANCE,
			JedisClusterSupplier.INSTANCE);

	@Override
	public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {
		return SUPPORTED_INJECTABLE_TYPES.contains(parameterContext.getParameter().getType());
	}

	/**
	 * Attempt to resolve the {@code requestedResourceType}.
	 *
	 * @param extensionContext
	 * @param requestedResourceType
	 * @param <T>
	 * @return
	 */
	public <T> T resolve(ExtensionContext extensionContext, Class<T> requestedResourceType) {

		ExtensionContext.Store store = getStore(extensionContext);

		return (T) store.getOrComputeIfAbsent(requestedResourceType, it -> findSupplier(requestedResourceType).get());
	}

	@Override
	public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
			throws ParameterResolutionException {

		ExtensionContext.Store store = getStore(extensionContext);
		Parameter parameter = parameterContext.getParameter();
		Type parameterizedType = parameter.getParameterizedType();

		return store.getOrComputeIfAbsent(parameter.getType(), it -> doGetInstance(parameterizedType));
	}

	@SuppressWarnings("unchecked")
	private static Supplier<Object> findSupplier(Type type) {

		ResolvableType requested = ResolvableType.forType(type);

		Supplier<?> supplier = SUPPLIERS.stream().filter(it -> {

			ResolvableType providedType = ResolvableType.forType(it.getClass()).as(Supplier.class).getGeneric(0);

			if (requested.isAssignableFrom(providedType)) {
				return true;
			}
			return false;
		}).findFirst().orElseThrow(() -> new NoSuchElementException("Cannot find a factory for " + type));

		return (Supplier) supplier;
	}

	private Object doGetInstance(Type parameterizedType) {
		return findSupplier(parameterizedType).get();
	}

	private ExtensionContext.Store getStore(ExtensionContext extensionContext) {
		return extensionContext.getStore(NAMESPACE);
	}

	static class ResourceFunction {

		final ResolvableType dependsOn;
		final ResolvableType provides;
		final Function<Object, Object> function;

		public ResourceFunction(ResolvableType dependsOn, ResolvableType provides, Function<?, ?> function) {
			this.dependsOn = dependsOn;
			this.provides = provides;
			this.function = (Function) function;
		}
	}

	enum JedisSupplier implements Supplier<Jedis> {

		INSTANCE;

		final Lazy<Jedis> lazy = Lazy.of(() -> {
			Jedis client = new Jedis(SettingsUtils.getHost(), SettingsUtils.getPort());

			ShutdownQueue.INSTANCE.register(client);
			return client;
		});

		@Override
		public Jedis get() {
			return lazy.get();
		}
	}

	enum JedisClusterSupplier implements Supplier<JedisCluster> {

		INSTANCE;

		final Lazy<JedisCluster> lazy = Lazy.of(() -> {
			JedisCluster client = new JedisCluster(new HostAndPort(SettingsUtils.getHost(), SettingsUtils.getClusterPort()));

			ShutdownQueue.INSTANCE.register(client);
			return client;
		});

		@Override
		public JedisCluster get() {
			return lazy.get();
		}
	}

}
