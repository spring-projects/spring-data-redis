/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import reactor.test.StepVerifier;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientConfiguration;

/**
 * Integration tests for {@link ReactiveStringRedisTemplate}.
 *
 * @author Mark Paluch
 */
public class ReactiveStringRedisTemplateIntegrationTests {

	private static ReactiveRedisConnectionFactory connectionFactory;

	private ReactiveStringRedisTemplate template;

	@BeforeClass
	public static void beforeClass() {

		LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(SettingsUtils.standaloneConfiguration(),
				LettuceTestClientConfiguration.create());
		connectionFactory.afterPropertiesSet();
		ConnectionFactoryTracker.add(connectionFactory);

		ReactiveStringRedisTemplateIntegrationTests.connectionFactory = connectionFactory;
	}

	@Before
	public void before() {

		template = new ReactiveStringRedisTemplate(connectionFactory);

		StepVerifier.create(template.execute(connection -> connection.serverCommands().flushDb())).expectNext("OK")
				.verifyComplete();
	}

	@Test // DATAREDIS-643
	public void shouldSetAndGetKeys() {

		StepVerifier.create(template.opsForValue().set("key", "value")).expectNext(true).verifyComplete();
		StepVerifier.create(template.opsForValue().get("key")).expectNext("value").verifyComplete();
	}
}
