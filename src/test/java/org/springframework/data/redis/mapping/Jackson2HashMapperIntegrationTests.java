/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.redis.mapping;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link Jackson2HashMapper}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("params")
public class Jackson2HashMapperIntegrationTests {

	RedisTemplate<String, Object> template;
	RedisConnectionFactory factory;
	Jackson2HashMapper mapper;

	public Jackson2HashMapperIntegrationTests(RedisConnectionFactory factory) throws Exception {

		this.factory = factory;
		if (factory instanceof InitializingBean) {
			((InitializingBean) factory).afterPropertiesSet();
		}
	}

	public static Collection<RedisConnectionFactory> params() {

		return Arrays.asList(JedisConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class),
				LettuceConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class));
	}

	@BeforeEach
	public void setUp() {

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		template.afterPropertiesSet();

		this.mapper = new Jackson2HashMapper(true);
	}

	@ParameterizedRedisTest // DATAREDIS-423
	public void shouldWriteReadHashCorrectly() {

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		template.opsForHash().putAll("JON-SNOW", mapper.toHash(jon));

		Person result = (Person) mapper.fromHash(template.<String, Object> opsForHash().entries("JON-SNOW"));
		assertThat(result).isEqualTo(jon);
	}
}
