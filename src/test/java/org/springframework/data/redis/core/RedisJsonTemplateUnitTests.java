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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;
import org.springframework.data.redis.serializer.RedisJsonSerializer;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Unit tests for {@link RedisJsonTemplate} verifying construction, argument validation and command delegation against a
 * mocked {@link RedisConnection}, without requiring a running Redis instance.
 *
 * @author Yordan Tsintsov
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class RedisJsonTemplateUnitTests {

	@Mock
	RedisConnectionFactory connectionFactory;
	@Mock
	RedisSerializer<String> keySerializer;
	@Mock
	RedisJsonSerializer jsonSerializer;

	private RedisJsonTemplate<String> template;

	@BeforeEach
	void setUp() {
		this.template = new RedisJsonTemplate<>(connectionFactory, keySerializer, jsonSerializer);
	}

	@Test
		// GH-3390
	void defaultConstructorUsesJdkKeySerializer() {

		RedisJsonTemplate<String> template = new RedisJsonTemplate<>(connectionFactory);

		assertThat(template.getKeySerializer()).isInstanceOf(JdkSerializationRedisSerializer.class);
	}

	@Test
		// GH-3390
	void defaultConstructorUsesJacksonJsonSerializer() {

		RedisJsonTemplate<String> template = new RedisJsonTemplate<>(connectionFactory);

		assertThat(template.getJsonSerializer()).isInstanceOf(GenericJacksonJsonRedisSerializer.class);
	}

	@Test
		// GH-3390
	void gettersReturnConfiguredSerializers() {

		assertThat(template.getKeySerializer()).isSameAs(keySerializer);
		assertThat(template.getJsonSerializer()).isSameAs(jsonSerializer);
	}

	@Test
		// GH-3390
	void defaultConstructorRejectsNullConnectionFactory() {
		assertThatIllegalArgumentException().isThrownBy(() -> new RedisJsonTemplate<>(null));
	}

	@Test
		// GH-3390
	void constructorRejectsNullConnectionFactory() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RedisJsonTemplate<>(null, keySerializer, jsonSerializer));
	}

	@Test
		// GH-3390
	void constructorRejectsNullKeySerializer() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RedisJsonTemplate<String>(connectionFactory, null, jsonSerializer));
	}

	@Test
		// GH-3390
	void constructorRejectsNullJsonSerializer() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new RedisJsonTemplate<>(connectionFactory, keySerializer, null));
	}

	@Test
		// GH-3390
	void stringTemplateDefaultConstructorUsesStringKeySerializer() {

		StringRedisJsonTemplate template = new StringRedisJsonTemplate(connectionFactory);

		assertThat(template.getKeySerializer()).isSameAs(StringRedisSerializer.UTF_8);
	}

	@Test
		// GH-3390
	void stringTemplateDefaultConstructorUsesJacksonJsonSerializer() {

		StringRedisJsonTemplate template = new StringRedisJsonTemplate(connectionFactory);

		assertThat(template.getJsonSerializer()).isInstanceOf(GenericJacksonJsonRedisSerializer.class);
	}

	@Test
		// GH-3390
	void stringTemplateConstructorUsesConfiguredSerializers() {

		StringRedisJsonTemplate template = new StringRedisJsonTemplate(connectionFactory, keySerializer, jsonSerializer);

		assertThat(template.getKeySerializer()).isSameAs(keySerializer);
		assertThat(template.getJsonSerializer()).isSameAs(jsonSerializer);
	}

	@Test
		// GH-3390
	void stringTemplateDefaultConstructorRejectsNullConnectionFactory() {
		assertThatIllegalArgumentException().isThrownBy(() -> new StringRedisJsonTemplate(null));
	}

	@Test
		// GH-3390
	void stringTemplateConstructorRejectsNullConnectionFactory() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new StringRedisJsonTemplate(null, keySerializer, jsonSerializer));
	}

	@Test
		// GH-3390
	void stringTemplateConstructorRejectsNullKeySerializer() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new StringRedisJsonTemplate(connectionFactory, null, jsonSerializer));
	}

	@Test
		// GH-3390
	void stringTemplateConstructorRejectsNullJsonSerializer() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> new StringRedisJsonTemplate(connectionFactory, keySerializer, null));
	}

}
