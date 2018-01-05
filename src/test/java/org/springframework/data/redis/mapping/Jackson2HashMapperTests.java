/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.mapping;

import java.util.Arrays;
import java.util.Collection;

import org.hamcrest.core.Is;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;

/**
 * Integration tests for {@link Jackson2HashMapper}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class Jackson2HashMapperTests {

	RedisTemplate<String, Object> template;
	RedisConnectionFactory factory;
	Jackson2HashMapper mapper;

	public Jackson2HashMapperTests(RedisConnectionFactory factory) throws Exception {

		this.factory = factory;
		if (factory instanceof InitializingBean) {
			((InitializingBean) factory).afterPropertiesSet();
		}

		ConnectionFactoryTracker.add(factory);
	}

	@Parameters
	public static Collection<RedisConnectionFactory> params() {

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		return Arrays.<RedisConnectionFactory> asList(new JedisConnectionFactory(), lettuceConnectionFactory);
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		template.afterPropertiesSet();

		this.mapper = new Jackson2HashMapper(true);
	}

	@Test // DATAREDIS-423
	public void shouldWriteReadHashCorrectly() {

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		template.opsForHash().putAll("JON-SNOW", mapper.toHash(jon));

		Person result = (Person) mapper.fromHash(template.<String, Object> opsForHash().entries("JON-SNOW"));
		Assert.assertThat(result, Is.is(jon));
	}

}
