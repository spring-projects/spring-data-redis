/*
 * Copyright 2017 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.ByteBufferObjectFactory;
import org.springframework.data.redis.DoubleObjectFactory;
import org.springframework.data.redis.LongObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * Parameters for testing implementations of {@link ReactiveRedisTemplate}
 *
 * @author Mark Paluch
 */
abstract public class ReactiveOperationsTestParams {

	public static Collection<Object[]> testParams() {

		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Long> longFactory = new LongObjectFactory();
		ObjectFactory<Double> doubleFactory = new DoubleObjectFactory();
		ObjectFactory<ByteBuffer> rawFactory = new ByteBufferObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		// XStream serializer
		XStreamMarshaller xstream = new XStreamMarshaller();
		try {
			xstream.afterPropertiesSet();
		} catch (Exception ex) {
			throw new RuntimeException("Cannot init XStream", ex);
		}

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setPort(SettingsUtils.getPort());
		lettuceConnectionFactory.setHostName(SettingsUtils.getHost());
		lettuceConnectionFactory.afterPropertiesSet();

		ReactiveRedisTemplate<Object, Object> objectTemplate = new ReactiveRedisTemplate<>();
		objectTemplate.setConnectionFactory(lettuceConnectionFactory);
		objectTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<String, String> stringTemplate = new ReactiveRedisTemplate<>();
		stringTemplate.setDefaultSerializer(new StringRedisSerializer());
		stringTemplate.setEnableDefaultSerializer(true);
		stringTemplate.setConnectionFactory(lettuceConnectionFactory);
		stringTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<String, Long> longTemplate = new ReactiveRedisTemplate<>();
		longTemplate.setKeySerializer(new StringRedisSerializer());
		longTemplate.setValueSerializer(new GenericToStringSerializer<>(Long.class));
		longTemplate.setConnectionFactory(lettuceConnectionFactory);
		longTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<String, Double> doubleTemplate = new ReactiveRedisTemplate<>();
		doubleTemplate.setKeySerializer(new StringRedisSerializer());
		doubleTemplate.setValueSerializer(new GenericToStringSerializer<>(Double.class));
		doubleTemplate.setConnectionFactory(lettuceConnectionFactory);
		doubleTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<byte[], byte[]> rawTemplate = new ReactiveRedisTemplate<>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(lettuceConnectionFactory);
		rawTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<String, Person> personTemplate = new ReactiveRedisTemplate<>();
		personTemplate.setConnectionFactory(lettuceConnectionFactory);
		personTemplate.afterPropertiesSet();

		OxmSerializer serializer = new OxmSerializer(xstream, xstream);
		ReactiveRedisTemplate<String, String> xstreamStringTemplate = new ReactiveRedisTemplate<>();
		xstreamStringTemplate.setConnectionFactory(lettuceConnectionFactory);
		xstreamStringTemplate.setDefaultSerializer(serializer);
		xstreamStringTemplate.afterPropertiesSet();

		ReactiveRedisTemplate<String, Person> xstreamPersonTemplate = new ReactiveRedisTemplate<>();
		xstreamPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		xstreamPersonTemplate.setValueSerializer(serializer);
		xstreamPersonTemplate.afterPropertiesSet();

		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		ReactiveRedisTemplate<String, Person> jackson2JsonPersonTemplate = new ReactiveRedisTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		jackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		GenericJackson2JsonRedisSerializer genericJackson2JsonSerializer = new GenericJackson2JsonRedisSerializer();
		ReactiveRedisTemplate<String, Person> genericJackson2JsonPersonTemplate = new ReactiveRedisTemplate<>();
		genericJackson2JsonPersonTemplate.setConnectionFactory(lettuceConnectionFactory);
		genericJackson2JsonPersonTemplate.setValueSerializer(genericJackson2JsonSerializer);
		genericJackson2JsonPersonTemplate.afterPropertiesSet();

		return Arrays.asList(new Object[][] { //
				{ stringTemplate, stringFactory, stringFactory , "String"}, //
				{ objectTemplate, personFactory, personFactory , "Person/JDK"}, //
				{ longTemplate, stringFactory, longFactory , "Long"}, //
				{ doubleTemplate, stringFactory, doubleFactory , "Double"}, //
				{ rawTemplate, rawFactory, rawFactory , "raw"}, //
				{ personTemplate, stringFactory, personFactory , "String/Person/JDK"}, //
				{ xstreamStringTemplate, stringFactory, stringFactory , "String/OXM"}, //
				{ xstreamPersonTemplate, stringFactory, personFactory, "String/Person/OXM"}, //
				{ jackson2JsonPersonTemplate, stringFactory, personFactory, "Jackson2"}, //
				{ genericJackson2JsonPersonTemplate, stringFactory, personFactory, "Generic Jackson 2" } });
	}
}
