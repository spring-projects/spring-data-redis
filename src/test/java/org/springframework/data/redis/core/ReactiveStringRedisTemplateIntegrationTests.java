/*
 * Copyright 2017-2022 the original author or authors.
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

import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;

/**
 * Integration tests for {@link ReactiveStringRedisTemplate}.
 *
 * @author Mark Paluch
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
public class ReactiveStringRedisTemplateIntegrationTests {

	private ReactiveRedisConnectionFactory connectionFactory;

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
}
