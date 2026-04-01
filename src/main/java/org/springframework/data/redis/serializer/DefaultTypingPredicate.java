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
 * @author Chris Bono
 * @since 4.1
 */
@FunctionalInterface
public interface DefaultTypingPredicate {

	enum Action {
		YES,
		NO,
		DONT_CARE;
	}

	/**
	 * Determine the action to take for a particular type.
	 * @param clazz the type to check.
	 * @return the action to take.
	 */
	Action test(Class<?> clazz);

	/**
	 * Obtain a builder with no defaults configured.
	 * @return a builder with no defaults configured.
	 */
	static DefaultTypingPredicate.Builder empty() {
		return new StdDefaultTypingPredicate.DefaultBuilder();
	}

	/**
	 * Obtain a builder with defaults configured.
	 * @return a builder with defaults configured.
	 */
	static DefaultTypingPredicate.Builder defaults() {

		DefaultTypingPredicate.Builder builder = new StdDefaultTypingPredicate.DefaultBuilder();
		builder.include((clazz) -> clazz == Object.class);
		builder.exclude((clazz) -> Modifier.isFinal(clazz.getModifiers()) && clazz.getPackageName().startsWith("java"));
		builder.exclude(ClassUtils::isPrimitiveOrWrapper);
		builder.include(Class::isEnum);
		builder.include(Class::isRecord);

		return builder;
	}

	/**
	 *
	 */
	interface Builder {

		Builder include(Predicate<Class<?>> typeMatcher);

		Builder exclude(Predicate<Class<?>> typeMatcher);

		Builder dontCare(Predicate<Class<?>> typeMatcher);

		DefaultTypingPredicate build();
	}

}
