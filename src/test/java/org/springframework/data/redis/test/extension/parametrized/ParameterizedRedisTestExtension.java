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

import static org.junit.platform.commons.util.AnnotationUtils.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.support.AnnotationConsumerInitializer;
import org.junit.platform.commons.JUnitException;
import org.junit.platform.commons.util.ExceptionUtils;
import org.junit.platform.commons.util.Preconditions;
import org.junit.platform.commons.util.ReflectionUtils;

/**
 * Copy of {@code org.junit.jupiter.params.ParameterizedTestExtension}.
 */
class ParameterizedRedisTestExtension implements TestTemplateInvocationContextProvider {

	private static final String METHOD_CONTEXT_KEY = "method-context";
	private static final String CONSTRUCTOR_CONTEXT_KEY = "constructor-context";
	static final String ARGUMENT_MAX_LENGTH_KEY = "junit.jupiter.params.displayname.argument.maxlength";

	@Override
	public boolean supportsTestTemplate(ExtensionContext context) {
		if (!context.getTestMethod().isPresent()) {
			return false;
		}

		Method testMethod = context.getTestMethod().get();
		if (!isAnnotated(testMethod, ParameterizedRedisTest.class)) {
			return false;
		}

		Constructor<?> declaredConstructor = ReflectionUtils.getDeclaredConstructor(context.getRequiredTestClass());
		ParameterizedTestContext methodContext = new ParameterizedTestContext(testMethod);
		ParameterizedTestContext constructorContext = new ParameterizedTestContext(declaredConstructor);

		Preconditions.condition(methodContext.hasPotentiallyValidSignature(),
				() -> String.format("@ParameterizedRedisTest method [%s] declares formal parameters in an invalid order: "
						+ "argument aggregators must be declared after any indexed arguments "
						+ "and before any arguments resolved by another ParameterResolver.", testMethod.toGenericString()));

		getStore(context).put(METHOD_CONTEXT_KEY, methodContext);
		getStore(context).put(CONSTRUCTOR_CONTEXT_KEY, constructorContext);

		return true;
	}

	@Override
	public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
			ExtensionContext extensionContext) {

		Method templateMethod = extensionContext.getRequiredTestMethod();
		String displayName = extensionContext.getDisplayName();
		ParameterizedTestContext methodContext = getStore(extensionContext)//
				.get(METHOD_CONTEXT_KEY, ParameterizedTestContext.class);

		ParameterizedTestContext constructorContext = getStore(extensionContext)//
				.get(CONSTRUCTOR_CONTEXT_KEY, ParameterizedTestContext.class);
		int argumentMaxLength = extensionContext.getConfigurationParameter(ARGUMENT_MAX_LENGTH_KEY, Integer::parseInt)
				.orElse(512);
		ParameterizedTestNameFormatter formatter = createNameFormatter(templateMethod, methodContext, displayName,
				argumentMaxLength);
		AtomicLong invocationCount = new AtomicLong(0);

		List<Class<?>> hierarchy = new ArrayList<>();
		Class<?> type = extensionContext.getRequiredTestClass();
		while (type != Object.class) {
			hierarchy.add(type);
			type = type.getSuperclass();
		}

		// @formatter:off
		return hierarchy.stream().flatMap(it -> findRepeatableAnnotations(it, ArgumentsSource.class).stream()
				.map(ArgumentsSource::value).map(this::instantiateArgumentsProvider)
				.map(provider -> AnnotationConsumerInitializer.initialize(it, provider)))
				.flatMap(provider -> arguments(provider, extensionContext)).map(Arguments::get)
				.map(arguments -> consumedArguments(arguments, methodContext))
				.map(arguments -> createInvocationContext(formatter, constructorContext, methodContext, arguments))
				.peek(invocationContext -> invocationCount.incrementAndGet())
				.onClose(() -> Preconditions.condition(invocationCount.get() > 0,
						"Configuration error: You must configure at least one set of arguments for this @ParameterizedRedisTest class"));
		// @formatter:on
	}

	@SuppressWarnings("ConstantConditions")
	private ArgumentsProvider instantiateArgumentsProvider(Class<? extends ArgumentsProvider> clazz) {
		try {
			return ReflectionUtils.newInstance(clazz);
		} catch (Exception ex) {
			if (ex instanceof NoSuchMethodException) {
				String message = String.format("Failed to find a no-argument constructor for ArgumentsProvider [%s]; "
						+ "Please ensure that a no-argument constructor exists and "
						+ "that the class is either a top-level class or a static nested class", clazz.getName());
				throw new JUnitException(message, ex);
			}
			throw ex;
		}
	}

	private ExtensionContext.Store getStore(ExtensionContext context) {
		return context.getStore(Namespace.create(ParameterizedRedisTestExtension.class, context.getRequiredTestMethod()));
	}

	private TestTemplateInvocationContext createInvocationContext(ParameterizedTestNameFormatter formatter,
			ParameterizedTestContext constructorContext, ParameterizedTestContext methodContext, Object[] arguments) {
		return new ParameterizedTestInvocationContext(formatter, constructorContext, methodContext, arguments);
	}

	private ParameterizedTestNameFormatter createNameFormatter(Method templateMethod,
			ParameterizedTestContext methodContext, String displayName, int argumentMaxLength) {

		ParameterizedRedisTest parameterizedTest = findAnnotation(templateMethod, ParameterizedRedisTest.class).get();
		String pattern = Preconditions.notBlank(parameterizedTest.name().trim(),
				() -> String.format(
						"Configuration error: @ParameterizedRedisTest on method [%s] must be declared with a non-empty name.",
						templateMethod));
		return new ParameterizedTestNameFormatter(pattern, displayName, methodContext, argumentMaxLength);
	}

	protected static Stream<? extends Arguments> arguments(ArgumentsProvider provider, ExtensionContext context) {
		try {
			return provider.provideArguments(context);
		} catch (Exception ex) {
			throw ExceptionUtils.throwAsUncheckedException(ex);
		}
	}

	private Object[] consumedArguments(Object[] arguments, ParameterizedTestContext methodContext) {
		return arguments;
	}

}
