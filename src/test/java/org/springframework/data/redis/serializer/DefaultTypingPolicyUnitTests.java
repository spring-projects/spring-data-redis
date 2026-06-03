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

import java.util.Arrays;
import java.util.Collection;
import org.assertj.core.util.introspection.ClassUtils;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.serializer.DefaultTypingPolicy.Outcome;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link DefaultTypingPolicy}.
 *
 * @author Chris Bono
 */
class DefaultTypingPolicyUnitTests {

	@Test
	void emptyPolicyReturnsNoOpinionForAllTypes() {

		DefaultTypingPolicy policy = DefaultTypingPolicy.empty().build();

		assertThat(policy.outcomeForType(String.class)).isEqualTo(Outcome.NO_OPINION);
		assertThat(policy.outcomeForType(Collection.class)).isEqualTo(Outcome.NO_OPINION);
		assertThat(policy.outcomeForType(ClassType.class)).isEqualTo(Outcome.NO_OPINION);
		assertThat(policy.outcomeForType(RecordType.class)).isEqualTo(Outcome.NO_OPINION);
		assertThat(policy.outcomeForType(EnumType.class)).isEqualTo(Outcome.NO_OPINION);
	}

	@Test
	void policyWithBasicRules() {

		DefaultTypingPolicy policy = DefaultTypingPolicy.empty()
				.include(Class::isEnum)
				.include(Class::isRecord)
				.exclude(ClassUtils::isPrimitiveOrWrapper)
				.exclude(ClassUtils::isInJavaLangPackage)
				.build();

		assertThat(policy.outcomeForType(ClassType.class)).isEqualTo(Outcome.NO_OPINION);
		assertThat(policy.outcomeForType(EnumType.class)).isEqualTo(Outcome.INCLUDE_TYPE_HINT);
		assertThat(policy.outcomeForType(RecordType.class)).isEqualTo(Outcome.INCLUDE_TYPE_HINT);
		assertThat(policy.outcomeForType(int.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);
		assertThat(policy.outcomeForType(Integer.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);
		assertThat(policy.outcomeForType(String.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);
	}

	@Test
	void policyRulesCanBeOverridden() {

		DefaultTypingPolicy policy = DefaultTypingPolicy.empty()
				.include(Class::isEnum)
				.include(Class::isRecord)
				.exclude(Class::isEnum)
				.build();

		assertThat(policy.outcomeForType(ClassType.class)).isEqualTo(Outcome.NO_OPINION);
		assertThat(policy.outcomeForType(EnumType.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);
		assertThat(policy.outcomeForType(RecordType.class)).isEqualTo(Outcome.INCLUDE_TYPE_HINT);
	}

	@Test
	void defaultPolicyRulesAsExpected() {

		DefaultTypingPolicy policy = DefaultTypingPolicy.defaults().build();

		// java.lang.Object included
		assertThat(policy.outcomeForType(Object.class)).isEqualTo(Outcome.INCLUDE_TYPE_HINT);

		// Final java.* type excluded
		assertThat(policy.outcomeForType(Arrays.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);

		// Non-final java.* type no opinion
		assertThat(policy.outcomeForType(RuntimeException.class)).isEqualTo(Outcome.NO_OPINION);

		// Enums included
		assertThat(policy.outcomeForType(EnumType.class)).isEqualTo(Outcome.INCLUDE_TYPE_HINT);

		// Records included
		assertThat(policy.outcomeForType(RecordType.class)).isEqualTo(Outcome.INCLUDE_TYPE_HINT);

		// Primitive and wrappers excluded
		assertThat(policy.outcomeForType(int.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);
		assertThat(policy.outcomeForType(Integer.class)).isEqualTo(Outcome.EXCLUDE_TYPE_HINT);
	}

	class ClassType {
	}

	record RecordType(String name) {
	}

	enum EnumType {
		ONE, TWO;
	}
}
