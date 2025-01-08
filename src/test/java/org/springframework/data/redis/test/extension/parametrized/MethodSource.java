/*
 * Copyright 2020-2025 the original author or authors.
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

import org.junit.jupiter.params.provider.ArgumentsSource;

/**
 * {@code @MethodSource} is an {@link ArgumentsSource} which provides access to values returned from
 * {@linkplain #value() factory methods} of the class in which this annotation is declared or from static factory
 * methods in external classes referenced by <em>fully qualified method name</em>.
 * <p>
 * This variant can be used only on type level.
 * <p>
 * Copy of {@code org.junit.jupiter.params.provider.MethodSource}.
 */
@Target({ ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ArgumentsSource(MethodArgumentsProvider.class)
public @interface MethodSource {

	/**
	 * The names of factory methods within the test class or in external classes to use as sources for arguments.
	 * <p>
	 * Factory methods in external classes must be referenced by <em>fully qualified method name</em> &mdash; for example,
	 * {@code com.example.StringsProviders#blankStrings}.
	 * <p>
	 * If no factory method names are declared, a method within the test class that has the same name as the test method
	 * will be used as the factory method by default.
	 * <p>
	 * For further information, see the {@linkplain MethodSource class-level Javadoc}.
	 */
	String[] value() default "";

}
