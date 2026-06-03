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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.springframework.data.redis.serializer.DefaultTypingPolicy.Outcome.INCLUDE_TYPE_HINT;
import static org.springframework.data.redis.serializer.DefaultTypingPolicy.Outcome.EXCLUDE_TYPE_HINT;
import static org.springframework.data.redis.serializer.DefaultTypingPolicy.Outcome.NO_OPINION;

/**
 * The default implementation of {@link DefaultTypingPolicy},
 * as created by the static factory methods.
 *
 * @see DefaultTypingPolicy#empty()
 * @see DefaultTypingPolicy#defaults()
 */
final class StdDefaultTypingPolicy implements DefaultTypingPolicy {

	private final List<JavaTypeSpec> typeSpecs = new ArrayList<>();

	StdDefaultTypingPolicy(List<JavaTypeSpec> typeSpecs) {
		this.typeSpecs.addAll(typeSpecs);
	}

	/**
	 * Determine the outcome to take for a particular type.
	 * <p>
	 * <b>Important:</b> All rules are evaluated in the order they are
	 * added, with the last matching winning. If no outcome is matched
	 * for a particular type, the default {@link Outcome#NO_OPINION NO_OPINION}
	 * outcome is used.
	 *
	 * @param clazz the type to check.
	 * @return the outcome for the type.
	 */
	@Override
	public Outcome outcomeForType(Class<?> clazz) {

		List<Outcome> results = typeSpecs.stream()
				.filter((typeSpec) -> typeSpec.typeMatcher().test(clazz))
				.map(JavaTypeSpec::outcome)
				.toList();

		return results.isEmpty() ? NO_OPINION : results.get(results.size() - 1);
	}

	/**
	 * The default {@link DefaultTypingPolicy.Builder builder} implementation that
	 * builds an ordered list of rules to apply.
	 */
	static final class DefaultBuilder implements DefaultTypingPolicy.Builder {

		private final List<JavaTypeSpec> typeSpecs = new ArrayList<>();

		@Override
		public DefaultTypingPolicy.Builder include(Predicate<Class<?>> typeMatcher) {
			typeSpecs.add(new JavaTypeSpec(typeMatcher, INCLUDE_TYPE_HINT));
			return this;
		}

		@Override
		public DefaultTypingPolicy.Builder exclude(Predicate<Class<?>> typeMatcher) {
			typeSpecs.add(new JavaTypeSpec(typeMatcher, EXCLUDE_TYPE_HINT));
			return this;
		}

		@Override
		public DefaultTypingPolicy.Builder fallback(Predicate<Class<?>> typeMatcher) {
			typeSpecs.add(new JavaTypeSpec(typeMatcher, NO_OPINION));
			return this;
		}

		@Override
		public DefaultTypingPolicy build() {
			return new StdDefaultTypingPolicy(typeSpecs);
		}

	}

	record JavaTypeSpec(Predicate<Class<?>> typeMatcher, Outcome outcome) {
	}
}
