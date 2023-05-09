/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.redis.test.extension.parametrized;

import static java.lang.String.*;
import static org.junit.jupiter.params.provider.Arguments.*;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.CollectionUtils;
import org.junit.platform.commons.util.Preconditions;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.StringUtils;
import org.springframework.data.redis.ConnectionFactoryTracker.Managed;

/**
 * Copy of {@code org.junit.jupiter.params.provider.MethodArgumentsProvider}.
 */
class MethodArgumentsProvider implements ArgumentsProvider, AnnotationConsumer<MethodSource> {

	private final static Namespace NAMESPACE = Namespace.create(MethodArgumentsProvider.class);
	private String[] methodNames = new String[0];

	@Override
	public void accept(MethodSource annotation) {
		this.methodNames = annotation.value();
	}

	@Override
	public Stream<Arguments> provideArguments(ExtensionContext context) {

		Store store = context.getRoot().getStore(NAMESPACE);
		Object testInstance = context.getTestInstance().orElse(null);
		return Arrays.stream(this.methodNames).map(factoryMethodName -> getMethod(context, factoryMethodName))
				.map(method -> (CloseablePararmeters) store.getOrComputeIfAbsent(new SourceKey(method, testInstance),
						key -> new CloseablePararmeters(ReflectionUtils.invokeMethod(method, testInstance), context)))
				.map(CloseablePararmeters::parameters).flatMap(CollectionUtils::toStream)
				.map(MethodArgumentsProvider::toArguments);
	}

	private Method getMethod(ExtensionContext context, String factoryMethodName) {
		if (StringUtils.isNotBlank(factoryMethodName)) {
			if (factoryMethodName.contains("#")) {
				return getMethodByFullyQualifiedName(factoryMethodName);
			} else {
				return ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), factoryMethodName);
			}
		}
		return ReflectionUtils.getRequiredMethod(context.getRequiredTestClass(), context.getRequiredTestMethod().getName());
	}

	private Method getMethodByFullyQualifiedName(String fullyQualifiedMethodName) {
		String[] methodParts = ReflectionUtils.parseFullyQualifiedMethodName(fullyQualifiedMethodName);
		String className = methodParts[0];
		String methodName = methodParts[1];
		String methodParameters = methodParts[2];

		Preconditions.condition(StringUtils.isBlank(methodParameters),
				() -> format("factory method [%s] must not declare formal parameters", fullyQualifiedMethodName));

		return ReflectionUtils.getRequiredMethod(loadRequiredClass(className), methodName);
	}

	private Class<?> loadRequiredClass(String className) {
		return ReflectionUtils.tryToLoadClass(className)
				.getOrThrow(cause -> new JUnitException(format("Could not load class [%s]", className), cause));
	}

	private static Arguments toArguments(Object item) {

		// Nothing to do except cast.
		if (item instanceof Arguments) {
			return (Arguments) item;
		}

		// Pass all multidimensional arrays "as is", in contrast to Object[].
		// See https://github.com/junit-team/junit5/issues/1665
		if (ReflectionUtils.isMultidimensionalArray(item)) {
			return arguments(item);
		}

		// Special treatment for one-dimensional reference arrays.
		// See https://github.com/junit-team/junit5/issues/1665
		if (item instanceof Object[]) {
			return arguments((Object[]) item);
		}

		// Pass everything else "as is".
		return arguments(item);
	}

	/**
	 * Key type for a method associated with a test instance.
	 *
	 * @param method
	 * @param instance
	 */
	record SourceKey(Method method, Object instance) {
	}

	/**
	 * Holder for parameters that can be closed using JUnit's built-in cleanup mechanism.
	 *
	 * @param parameters
	 * @param context
	 */
	record CloseablePararmeters(Object parameters, Object context) implements Store.CloseableResource {

		@Override
		public void close() {
			close0(parameters);
		}

		private void close0(Object object) {
			if (object instanceof Managed) {
				return;
			}

			if (object instanceof CloseableResource) {
				try {
					((CloseableResource) object).close();
				} catch (Throwable e) {
					throw new RuntimeException(e);
				}

				return;
			}

			if (object instanceof Closeable) {
				try {
					((AutoCloseable) object).close();
				} catch (Throwable e) {
					throw new RuntimeException(e);
				}
				return;
			}

			if (object instanceof Arguments) {
				close0(((Arguments) object).get());
			}

			if (object instanceof Object[]) {
				Arrays.asList((Object[]) object).forEach(this::close0);
			}

			if (object instanceof Iterable<?>) {
				((Iterable<Object>) object).forEach(this::close0);
			}
		}
	}

}
