/*
 * Copyright 2013-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Parameters for testing implementations of {@link AbstractOperations}
 *
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
abstract public class AbstractOperationsTestParams {

	// DATAREDIS-241
	public static Collection<Object[]> testParams() {

		List<Object[]> params = new ArrayList<>();
		params.addAll(testParams(LettuceConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class)));
		params.addAll(testParams(JedisConnectionFactoryExtension.getConnectionFactory(RedisStandalone.class)));
		return params;
	}

	// DATAREDIS-241
	public static Collection<Object[]> testParams(RedisConnectionFactory connectionFactory) {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate();
		stringTemplate.setConnectionFactory(connectionFactory);
		stringTemplate.afterPropertiesSet();

		RedisTemplate<String, Long> longTemplate = new RedisTemplate<>();
		longTemplate.setKeySerializer(StringRedisSerializer.UTF_8);
		longTemplate.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		longTemplate.setConnectionFactory(connectionFactory);
		longTemplate.afterPropertiesSet();

		RedisTemplate<String, Double> doubleTemplate = new RedisTemplate<>();
		doubleTemplate.setKeySerializer(StringRedisSerializer.UTF_8);
		doubleTemplate.setValueSerializer(new GenericToStringSerializer<>(Double.class));
		doubleTemplate.setConnectionFactory(connectionFactory);
		doubleTemplate.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> rawTemplate = new RedisTemplate<>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(connectionFactory);
		rawTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> personTemplate = new RedisTemplate<>();
		personTemplate.setConnectionFactory(connectionFactory);
		personTemplate.afterPropertiesSet();

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		RedisTemplate<String, String> xstreamStringTemplate = new RedisTemplate<>();
		xstreamStringTemplate.setConnectionFactory(connectionFactory);
		xstreamStringTemplate.setDefaultSerializer(serializer);
		xstreamStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplate = new RedisTemplate<>();
		xstreamPersonTemplate.setConnectionFactory(connectionFactory);
		xstreamPersonTemplate.setValueSerializer(serializer);
		xstreamPersonTemplate.afterPropertiesSet();

		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		RedisTemplate<String, Person> jackson2JsonPersonTemplate = new RedisTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(connectionFactory);
		jackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		JacksonJsonRedisSerializer<Person> jackson3JsonSerializer = new JacksonJsonRedisSerializer<>(Person.class);
		RedisTemplate<String, Person> jackson3JsonPersonTemplate = new RedisTemplate<>();
		jackson3JsonPersonTemplate.setConnectionFactory(connectionFactory);
		jackson3JsonPersonTemplate.setValueSerializer(jackson3JsonSerializer);
		jackson3JsonPersonTemplate.afterPropertiesSet();

		GenericJackson2JsonRedisSerializer genericJackson2JsonSerializer = new GenericJackson2JsonRedisSerializer();
		RedisTemplate<String, Person> genericJackson2JsonPersonTemplate = new RedisTemplate<>();
		genericJackson2JsonPersonTemplate.setConnectionFactory(connectionFactory);
		genericJackson2JsonPersonTemplate.setValueSerializer(genericJackson2JsonSerializer);
		genericJackson2JsonPersonTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { //
				{ stringTemplate, stringFactory, stringFactory }, //
				{ longTemplate, stringFactory, longFactory }, //
				{ doubleTemplate, stringFactory, doubleFactory }, //
				{ rawTemplate, rawFactory, rawFactory }, //
				{ personTemplate, stringFactory, personFactory }, //
				{ xstreamStringTemplate, stringFactory, stringFactory }, //
				{ xstreamPersonTemplate, stringFactory, personFactory }, //
				{ jackson2JsonPersonTemplate, stringFactory, personFactory }, //
				{ jackson3JsonPersonTemplate, stringFactory, personFactory }, //
				{ genericJackson2JsonPersonTemplate, stringFactory, personFactory } });
	}
}
