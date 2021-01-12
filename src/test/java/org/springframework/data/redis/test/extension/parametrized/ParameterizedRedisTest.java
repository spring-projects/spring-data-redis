/*
 * Copyright 2020-2021 the original author or authors.
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

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * {@code @ParameterizedRedisTest} is used to signal that the annotated method is a <em>parameterized test</em> method
 * within a potentially parametrized test class.
 * <p>
 * Such methods must not be {@code private} or {@code static}.
 * <h3>Argument Providers and Sources</h3>
 * <p>
 * Test classes defining {@code @ParameterizedRedisTest} methods must specify at least one
 * {@link org.junit.jupiter.params.provider.ArgumentsProvider ArgumentsProvider} via
 * {@link org.junit.jupiter.params.provider.ArgumentsSource @ArgumentsSource} or a corresponding composed annotation
 * (e.g., {@code @ValueSource}, {@code @CsvSource}, etc.). The provider is responsible for providing a
 * {@link java.util.stream.Stream Stream} of {@link org.junit.jupiter.params.provider.Arguments Arguments} that will be
 * used to invoke the parameterized test method.
 * <h3>Method Parameter List</h3>
 * <p>
 * A {@code @ParameterizedRedisTest} method may declare parameters that are resolved from the class-level
 * {@link org.junit.jupiter.params.provider.ArgumentsSource}. Additional parameters that exceed the parameter index by
 * the class argument source can be resolved by additional {@link org.junit.jupiter.api.extension.ParameterResolver
 * ParameterResolvers} (e.g., {@code TestInfo}, {@code TestReporter}, etc). Specifically, a parameterized test method
 * must declare formal parameters according to the following rules.
 * <ol>
 * <li>Zero or more <em>indexed arguments</em> must be declared first.</li>
 * <li>Zero or more <em>aggregators</em> must be declared next.</li>
 * <li>Zero or more arguments supplied by other {@code ParameterResolver} implementations must be declared last.</li>
 * </ol>
 * <p>
 * In this context, an <em>indexed argument</em> is an argument for a given index in the {@code Arguments} provided by
 * an {@code ArgumentsProvider} that is passed as an argument to the parameterized method at the same index in the
 * method's formal parameter list. An <em>aggregator</em> is any parameter of type
 * {@link org.junit.jupiter.params.aggregator.ArgumentsAccessor ArgumentsAccessor} or any parameter annotated with
 * {@link org.junit.jupiter.params.aggregator.AggregateWith @AggregateWith}.
 * <p>
 * <h3>Constructor Parameter List</h3>
 * <p>
 * A class defining {@code @ParameterizedRedisTest} may declare a constructor accepting arguments. Constructor arguments
 * are resolved from the same class-level {@link org.junit.jupiter.params.provider.ArgumentsSource} as method arguments.
 * Therefore, constructor arguments follow the same index/aggregator semantics as method arguments.
 * <h3>Test Class Lifecycle</h3>
 * <p>
 * Using parameterized tests requires instance creation per test method ({@code TestInstance(PER_METHOD)} to ensure that
 * both, test method and test instance share the same arguments. This limitation is driven by the execution tree as test
 * argument sources are activated per test method. See
 * {@link org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider#provideTestTemplateInvocationContexts(ExtensionContext)}.
 * <p>
 * Adaption of {@code org.junit.jupiter.params.ParameterizedTest}.
 */
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@TestTemplate
@ExtendWith(ParameterizedRedisTestExtension.class)
public @interface ParameterizedRedisTest {

	/**
	 * Placeholder for the {@linkplain org.junit.jupiter.api.TestInfo#getDisplayName display name} of a
	 * {@code @ParameterizedTest} method: <code>{displayName}</code>
	 *
	 * @see #name
	 */
	String DISPLAY_NAME_PLACEHOLDER = "{displayName}";

	/**
	 * Placeholder for the current invocation index of a {@code @ParameterizedTest} method (1-based): <code>{index}</code>
	 *
	 * @see #name
	 */
	String INDEX_PLACEHOLDER = "{index}";

	/**
	 * Placeholder for the complete, comma-separated arguments list of the current invocation of a
	 * {@code @ParameterizedTest} method: <code>{arguments}</code>
	 *
	 * @see #name
	 */
	String ARGUMENTS_PLACEHOLDER = "{arguments}";

	/**
	 * Placeholder for the complete, comma-separated named arguments list of the current invocation of a
	 * {@code @ParameterizedTest} method: <code>{argumentsWithNames}</code>
	 *
	 * @see #name
	 */
	String ARGUMENTS_WITH_NAMES_PLACEHOLDER = "{argumentsWithNames}";

	/**
	 * Default display name pattern for the current invocation of a {@code @ParameterizedTest} method: {@value}
	 * <p>
	 * Note that the default pattern does <em>not</em> include the {@linkplain #DISPLAY_NAME_PLACEHOLDER display name} of
	 * the {@code @ParameterizedTest} method.
	 *
	 * @see #name
	 * @see #DISPLAY_NAME_PLACEHOLDER
	 * @see #INDEX_PLACEHOLDER
	 * @see #ARGUMENTS_WITH_NAMES_PLACEHOLDER
	 */
	String DEFAULT_DISPLAY_NAME = "[" + INDEX_PLACEHOLDER + "] " + ARGUMENTS_WITH_NAMES_PLACEHOLDER;

	/**
	 * The display name to be used for individual invocations of the parameterized test; never blank or consisting solely
	 * of whitespace.
	 * <p>
	 * Defaults to {@link #DEFAULT_DISPLAY_NAME}.
	 * <h4>Supported placeholders</h4>
	 * <ul>
	 * <li>{@link #DISPLAY_NAME_PLACEHOLDER}</li>
	 * <li>{@link #INDEX_PLACEHOLDER}</li>
	 * <li>{@link #ARGUMENTS_PLACEHOLDER}</li>
	 * <li><code>{0}</code>, <code>{1}</code>, etc.: an individual argument (0-based)</li>
	 * </ul>
	 * <p>
	 * For the latter, you may use {@link java.text.MessageFormat} patterns to customize formatting. Please note that the
	 * original arguments are passed when formatting, regardless of any implicit or explicit argument conversions.
	 *
	 * @see java.text.MessageFormat
	 */
	String name() default DEFAULT_DISPLAY_NAME;

}
