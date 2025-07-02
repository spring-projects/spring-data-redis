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

import org.junit.jupiter.params.ParameterizedClass;

import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
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
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.XstreamOxmSerializerSingleton;
import org.springframework.data.redis.test.extension.RedisStandalone;

/**
 * Integration test for RedisMap.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ParameterizedClass
public class RedisMapIntegrationTests extends AbstractRedisMapIntegrationTests<Object, Object> {

	@SuppressWarnings("rawtypes")
	public RedisMapIntegrationTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory,
			RedisTemplate template) {
		super(keyFactory, valueFactory, template);
	}

	@SuppressWarnings("unchecked")
	RedisMap<Object, Object> createMap() {
		String redisName = getClass().getSimpleName();
		return new DefaultRedisMap<Object, Object>(redisName, template);
	}

	// DATAREDIS-241
	@SuppressWarnings("rawtypes")
	public static Collection<Object[]> testParams() {

		OxmSerializer serializer = XstreamOxmSerializerSingleton.getInstance();
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<>(Person.class);
		Jackson2JsonRedisSerializer<String> jackson2JsonStringSerializer = new Jackson2JsonRedisSerializer<>(
				String.class);
		StringRedisSerializer stringSerializer = StringRedisSerializer.UTF_8;

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnFactory = JedisConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class);

		RedisTemplate genericTemplate = new RedisTemplate();
		genericTemplate.setConnectionFactory(jedisConnFactory);
		genericTemplate.afterPropertiesSet();

		RedisTemplate<String, String> xstreamGenericTemplate = new RedisTemplate<>();
		xstreamGenericTemplate.setConnectionFactory(jedisConnFactory);
		xstreamGenericTemplate.setDefaultSerializer(serializer);
		xstreamGenericTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplate = new RedisTemplate<>();
		jackson2JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson2JsonPersonTemplate.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		RedisTemplate<String, byte[]> rawTemplate = new RedisTemplate<>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.setKeySerializer(stringSerializer);
		rawTemplate.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = LettuceConnectionFactoryExtension
				.getConnectionFactory(RedisStandalone.class, false);

		RedisTemplate genericTemplateLettuce = new RedisTemplate();
		genericTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		genericTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, Person> xGenericTemplateLettuce = new RedisTemplate<>();
		xGenericTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		xGenericTemplateLettuce.setDefaultSerializer(serializer);
		xGenericTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateLettuce = new RedisTemplate<>();
		jackson2JsonPersonTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLettuce.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLettuce.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLettuce.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate();
		stringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		stringTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, byte[]> rawTemplateLtc = new RedisTemplate<>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.setKeySerializer(stringSerializer);
		rawTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ personFactory, personFactory, genericTemplate }, //
				{ stringFactory, personFactory, genericTemplate }, //
				{ personFactory, stringFactory, genericTemplate }, //
				{ personFactory, stringFactory, xstreamGenericTemplate }, //
				{ personFactory, stringFactory, jackson2JsonPersonTemplate }, //
				{ rawFactory, rawFactory, rawTemplate }, //

				// lettuce
				{ stringFactory, stringFactory, genericTemplateLettuce }, //
				{ personFactory, personFactory, genericTemplateLettuce }, //
				{ stringFactory, personFactory, genericTemplateLettuce }, //
				{ personFactory, stringFactory, genericTemplateLettuce }, //
				{ personFactory, stringFactory, xGenericTemplateLettuce }, //
				{ personFactory, stringFactory, jackson2JsonPersonTemplateLettuce }, //
				{ stringFactory, doubleFactory, stringTemplateLtc }, //
				{ stringFactory, longFactory, stringTemplateLtc }, //
				{ rawFactory, rawFactory, rawTemplateLtc } });
	}
}
