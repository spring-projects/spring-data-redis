/*
 * Copyright 2017-present the original author or authors.
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

import reactor.test.StepVerifier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.RedisElementReader;
import org.springframework.data.redis.serializer.RedisElementWriter;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

/**
 * Integration tests for {@link ReactiveStringRedisTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
public class ReactiveStringRedisTemplateIntegrationTests {

	private final ReactiveRedisConnectionFactory connectionFactory;

	private ReactiveStringRedisTemplate template;

	public ReactiveStringRedisTemplateIntegrationTests(ReactiveRedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	@BeforeEach
	void before() {

		template = new ReactiveStringRedisTemplate(connectionFactory);

		template.execute(connection -> connection.serverCommands().flushDb()).as(StepVerifier::create).expectNext("OK")
				.verifyComplete();
	}

	@Test // DATAREDIS-643
	void shouldSetAndGetKeys() {

		template.opsForValue().set("key", "value").as(StepVerifier::create).expectNext(true).verifyComplete();
		template.opsForValue().get("key").as(StepVerifier::create).expectNext("value").verifyComplete();
	}

	@Test // GH-2655
	void keysFailsOnNullElements() {

		template.opsForValue().set("a", "1").as(StepVerifier::create).expectNext(true).verifyComplete();
		template.opsForValue().set("b", "1").as(StepVerifier::create).expectNext(true).verifyComplete();

		RedisElementReader<String> reader = RedisElementReader.from(StringRedisSerializer.UTF_8);
		RedisElementWriter<String> writer = RedisElementWriter.from(StringRedisSerializer.UTF_8);

		RedisSerializationContext<String, String> nullReadingContext = RedisSerializationContext
				.<String, String> newSerializationContext(StringRedisSerializer.UTF_8).key(buffer -> {

					String read = reader.read(buffer);

					return "a".equals(read) ? null : read;

				}, writer).build();

		ReactiveRedisTemplate<String, String> customTemplate = new ReactiveRedisTemplate<>(template.getConnectionFactory(),
				nullReadingContext);

		customTemplate.keys("b").as(StepVerifier::create).expectNext("b").verifyComplete();
		customTemplate.keys("a").as(StepVerifier::create).verifyError(InvalidDataAccessApiUsageException.class);
	}
}
