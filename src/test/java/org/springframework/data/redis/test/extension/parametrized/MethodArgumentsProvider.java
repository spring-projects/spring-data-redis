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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.CollectionUtils;
import org.junit.platform.commons.util.Preconditions;
import org.junit.platform.commons.util.ReflectionUtils;
import org.junit.platform.commons.util.StringUtils;

/**
 * Copy of {@code org.junit.jupiter.params.provider.MethodArgumentsProvider}.
 */
class MethodArgumentsProvider implements ArgumentsProvider, AnnotationConsumer<MethodSource> {

	private String[] methodNames = new String[0];

	@Override
	public void accept(MethodSource annotation) {
		this.methodNames = annotation.value();
	}

	@Override
	public Stream<Arguments> provideArguments(ExtensionContext context) {
		Object testInstance = context.getTestInstance().orElse(null);
		// @formatter:off
		return Arrays.stream(this.methodNames).map(factoryMethodName -> getMethod(context, factoryMethodName))
				.map(method -> ReflectionUtils.invokeMethod(method, testInstance)).flatMap(CollectionUtils::toStream)
				.map(MethodArgumentsProvider::toArguments);
		// @formatter:on
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

}
