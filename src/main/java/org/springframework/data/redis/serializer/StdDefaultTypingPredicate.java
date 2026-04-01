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

import static org.springframework.data.redis.serializer.DefaultTypingPredicate.Action.YES;
import static org.springframework.data.redis.serializer.DefaultTypingPredicate.Action.NO;
import static org.springframework.data.redis.serializer.DefaultTypingPredicate.Action.DONT_CARE;

/**
 * The default implementation of {@link DefaultTypingPredicate},
 * as created by the static factory methods.
 *
 * @see DefaultTypingPredicate#empty()
 * @see DefaultTypingPredicate#defaults()
 */
final class StdDefaultTypingPredicate implements DefaultTypingPredicate {

	private final List<JavaTypeSpec> typeSpecs = new ArrayList<>();

	StdDefaultTypingPredicate(List<JavaTypeSpec> typeSpecs) {
		this.typeSpecs.addAll(typeSpecs);
	}

	@Override
	public Action test(Class<?> clazz) {

		List<Action> results = typeSpecs.stream()
				.filter((typeSpec) -> typeSpec.typeMatcher().test(clazz))
				.map(JavaTypeSpec::action)
				.toList();

		return results.isEmpty() ? DONT_CARE : results.get(results.size() - 1);
	}

	static final class DefaultBuilder implements DefaultTypingPredicate.Builder {

		private final List<JavaTypeSpec> typeSpecs = new ArrayList<>();

		@Override
		public DefaultTypingPredicate.Builder include(Predicate<Class<?>> typeMatcher) {
			typeSpecs.add(new JavaTypeSpec(typeMatcher, YES));
			return this;
		}

		@Override
		public DefaultTypingPredicate.Builder exclude(Predicate<Class<?>> typeMatcher) {
			typeSpecs.add(new JavaTypeSpec(typeMatcher, NO));
			return this;
		}

		@Override
		public DefaultTypingPredicate.Builder dontCare(Predicate<Class<?>> typeMatcher) {
			typeSpecs.add(new JavaTypeSpec(typeMatcher, DONT_CARE));
			return this;
		}

		@Override
		public DefaultTypingPredicate build() {
			return new StdDefaultTypingPredicate(typeSpecs);
		}

	}

	record JavaTypeSpec(Predicate<Class<?>> typeMatcher, DefaultTypingPredicate.Action action) {
	}
}
