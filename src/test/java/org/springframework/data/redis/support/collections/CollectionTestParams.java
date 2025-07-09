/*
 * Copyright 2011-2025 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.Jackson3JsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public abstract class CollectionTestParams {

	public static Collection<Object[]> testParams() {

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		Jackson3JsonRedisSerializer<Person> jackson3JsonSerializer = new Jackson3JsonRedisSerializer<>(Person.class);
		StringRedisSerializer stringSerializer = StringRedisSerializer.UTF_8;

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<String> doubleAsStringObjectFactory = new DoubleAsStringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate(jedisConnFactory);
		RedisTemplate<String, Person> personTemplate = new RedisTemplate<>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();

		RedisTemplate<String, String> xstreamStringTemplate = new RedisTemplate<>();
		xstreamStringTemplate.setConnectionFactory(jedisConnFactory);
		xstreamStringTemplate.setDefaultSerializer(serializer);
		xstreamStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplate = new RedisTemplate<>();
		xstreamPersonTemplate.setConnectionFactory(jedisConnFactory);
		xstreamPersonTemplate.setValueSerializer(serializer);
		xstreamPersonTemplate.afterPropertiesSet();

		// jackson2
		RedisTemplate<String, Person> jackson2JsonPersonTemplate = new RedisTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson2JsonPersonTemplate.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> rawTemplate = new RedisTemplate<>();
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setKeySerializer(stringSerializer);
		rawTemplate.afterPropertiesSet();

		// jackson3
		RedisTemplate<String, Person> jackson3JsonPersonTemplate = new RedisTemplate<>();
		jackson3JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson3JsonPersonTemplate.setValueSerializer(jackson3JsonSerializer);
		jackson3JsonPersonTemplate.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate(lettuceConnFactory);
		RedisTemplate<String, Person> personTemplateLtc = new RedisTemplate<>();
		personTemplateLtc.setConnectionFactory(lettuceConnFactory);
		personTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamStringTemplateLtc = new RedisTemplate<>();
		xstreamStringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xstreamStringTemplateLtc.setDefaultSerializer(serializer);
		xstreamStringTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplateLtc = new RedisTemplate<>();
		xstreamPersonTemplateLtc.setValueSerializer(serializer);
		xstreamPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xstreamPersonTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateLtc = new RedisTemplate<>();
		jackson2JsonPersonTemplateLtc.setValueSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> jackson3JsonPersonTemplateLtc = new RedisTemplate<>();
		jackson3JsonPersonTemplateLtc.setValueSerializer(jackson3JsonSerializer);
		jackson3JsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jackson3JsonPersonTemplateLtc.afterPropertiesSet();

		RedisTemplate<byte[], byte[]> rawTemplateLtc = new RedisTemplate<>();
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setKeySerializer(stringSerializer);
		rawTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringTemplate }, //
				{ doubleAsStringObjectFactory, stringTemplate }, //
				{ personFactory, personTemplate }, //
				{ stringFactory, xstreamStringTemplate }, //
				{ personFactory, xstreamPersonTemplate }, //
				{ personFactory, jackson2JsonPersonTemplate }, //
				{ personFactory, jackson3JsonPersonTemplate }, //
				{ rawFactory, rawTemplate },

				// lettuce
				{ stringFactory, stringTemplateLtc }, //
				{ personFactory, personTemplateLtc }, //
				{ doubleAsStringObjectFactory, stringTemplateLtc }, //
				{ personFactory, personTemplateLtc }, //
				{ stringFactory, xstreamStringTemplateLtc }, //
				{ personFactory, xstreamPersonTemplateLtc }, //
				{ personFactory, jackson2JsonPersonTemplateLtc }, //
				{ personFactory, jackson3JsonPersonTemplateLtc }, //
				{ rawFactory, rawTemplateLtc } });
	}
}
