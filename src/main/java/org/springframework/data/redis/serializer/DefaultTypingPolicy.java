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
package org.springframework.data.redis.serializer;

import java.lang.reflect.Modifier;
import java.util.function.Predicate;
import org.springframework.util.ClassUtils;

/**
 * Policy that defines whether to include automatic type information for Jackson
 * for each serialized type.
 * <p>
 * Provides a {@link Builder builder} to create a composite policy consisting of
 * outcomes to apply for individual types.
 * <p>
 * An example that uses the default policy and adds a rule for a custom type:
 * <pre class="code">
 * DefaultTypingPolicy.defaults()
 *     .include((clazz) -> clazz == Person.class)
 *     .build();
 * </pre>
 * <p>
 * This is a {@link FunctionalInterface} whose functional method is
 * {@link #outcomeForType(Class)}.
 *
 * @author Chris Bono
 * @since 4.1
 */
public interface DefaultTypingPolicy {

	/**
	 * The outcome to apply for a given type.
	 */
	enum Outcome {

		/** Include type hints for the given type */
		INCLUDE_TYPE_HINT,

		/** Do not include type hints for the given type */
		EXCLUDE_TYPE_HINT,

		/** No opinion for the given type - fallback to the default logic */
		NO_OPINION;
	}

	/**
	 * Determine the outcome to take for a particular type.
	 * @param clazz the type to check.
	 * @return the outcome for the type.
	 */
	Outcome outcomeForType(Class<?> clazz);

	/**
	 * Obtain a builder with no defaults configured.
	 * @return a builder with no defaults configured.
	 */
	static DefaultTypingPolicy.Builder empty() {
		return new StdDefaultTypingPolicy.DefaultBuilder();
	}

	/**
	 * Obtain a builder with defaults configured.
	 * @return a builder with defaults configured.
	 */
	static DefaultTypingPolicy.Builder defaults() {

		DefaultTypingPolicy.Builder builder = new StdDefaultTypingPolicy.DefaultBuilder();
		builder.include((clazz) -> clazz == Object.class);
		builder.exclude((clazz) -> Modifier.isFinal(clazz.getModifiers()) && clazz.getPackageName().startsWith("java"));
		builder.exclude(ClassUtils::isPrimitiveOrWrapper);
		builder.include(Class::isEnum);
		builder.include(Class::isRecord);

		return builder;
	}

	/**
	 * A mutable builder for creating a {@link DefaultTypingPolicy}.
	 */
	interface Builder {

		/**
		 * Adds a rule that will return an {@link Outcome#INCLUDE_TYPE_HINT} outcome
		 * for matching types.
		 * @param typeMatcher the predicate to match the types.
		 * @return this.
		 */
		Builder include(Predicate<Class<?>> typeMatcher);

		/**
		 * Adds a rule that will return an {@link Outcome#EXCLUDE_TYPE_HINT} outcome
		 * for matching types.
		 * @param typeMatcher the predicate to match the types.
		 * @return this.
		 */
		Builder exclude(Predicate<Class<?>> typeMatcher);

		/**
		 * Adds a rule that will return an {@link Outcome#NO_OPINION} outcome
		 * for matching types.
		 * @param typeMatcher the predicate to match the types.
		 * @return this.
		 */
		Builder fallback(Predicate<Class<?>> typeMatcher);

		/**
		 * Build the policy.
		 * @return the policy.
		 */
		DefaultTypingPolicy build();
	}

}
