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
package org.springframework.data.redis.listener.support;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;

/**
 * Unit tests for {@link SimpleTopicResolver}.
 *
 * @author Christoph Strobl
 */
class SimpleTopicResolverUnitTests {

	private final SimpleTopicResolver resolver = new SimpleTopicResolver();

	@ParameterizedTest // GH-3339
	@ValueSource(strings = { "h?llo", "h*llo", "h[ae]llo", "news.*" })
	void resolvesRedisGlobAsPatternTopic(String destinationName) {
		assertThat(resolver.resolveTopic(destinationName)).isInstanceOf(PatternTopic.class);
	}

	@ParameterizedTest // GH-3339
	@ValueSource(strings = { "channel11", "ch:00", "plain", "foo", "literal.dot" })
	void resolvesNamesWithoutActiveGlobsAsChannelTopic(String destinationName) {
		assertThat(resolver.resolveTopic(destinationName)).isInstanceOf(ChannelTopic.class);
	}

	@ParameterizedTest // GH-3339
	@ValueSource(strings = { "file\\*name", "what\\?", "bracket\\[only", "trail\\" })
	void resolvesNamesWithOnlyEscapedGlobMetacharactersAsChannelTopic(String destinationName) {
		assertThat(resolver.resolveTopic(destinationName)).isInstanceOf(ChannelTopic.class);
	}

	@Test // GH-3339
	void resolvesEscapedGlobWithAdditionalUnescapedGlobAsPatternTopic() {
		assertThat(resolver.resolveTopic("abc.\\*foo.*")).isInstanceOf(PatternTopic.class);
	}
}
