/*
 * Copyright 2011-2016 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.PoolConfig;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * Integration test for RedisMap.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class RedisMapTests extends AbstractRedisMapTests<Object, Object> {

	@SuppressWarnings("rawtypes")
	public RedisMapTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory, RedisTemplate template) {
		super(keyFactory, valueFactory, template);
	}

	@SuppressWarnings("unchecked")
	RedisMap<Object, Object> createMap() {
		String redisName = getClass().getSimpleName();
		return new DefaultRedisMap<Object, Object>(redisName, template);
	}

	/**
	 * @see DATAREDIS-241
	 */
	@SuppressWarnings("rawtypes")
	@Parameters
	public static Collection<Object[]> testParams() {

		// XStream serializer
		XStreamMarshaller xstream = new XStreamMarshaller();
		try {
			xstream.afterPropertiesSet();
		} catch (Exception ex) {
			throw new RuntimeException("Cannot init XStream", ex);
		}
		OxmSerializer serializer = new OxmSerializer(xstream, xstream);
		JacksonJsonRedisSerializer<Person> jsonSerializer = new JacksonJsonRedisSerializer<Person>(Person.class);
		JacksonJsonRedisSerializer<String> jsonStringSerializer = new JacksonJsonRedisSerializer<String>(String.class);
		Jackson2JsonRedisSerializer<Person> jackson2JsonSerializer = new Jackson2JsonRedisSerializer<Person>(Person.class);
		Jackson2JsonRedisSerializer<String> jackson2JsonStringSerializer = new Jackson2JsonRedisSerializer<String>(
				String.class);
		StringRedisSerializer stringSerializer = new StringRedisSerializer();

		PoolConfig defaultPoolConfig = new PoolConfig();
		defaultPoolConfig.setMaxActive(1000);

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.getPoolConfig().setMaxTotal(defaultPoolConfig.getMaxTotal());
		jedisConnFactory.setUsePool(true);
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.afterPropertiesSet();

		RedisTemplate genericTemplate = new RedisTemplate();
		genericTemplate.setConnectionFactory(jedisConnFactory);
		genericTemplate.afterPropertiesSet();

		RedisTemplate<String, String> xstreamGenericTemplate = new RedisTemplate<String, String>();
		xstreamGenericTemplate.setConnectionFactory(jedisConnFactory);
		xstreamGenericTemplate.setDefaultSerializer(serializer);
		xstreamGenericTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplate = new RedisTemplate<String, Person>();
		jsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jsonPersonTemplate.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplate.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplate.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplate = new RedisTemplate<String, Person>();
		jackson2JsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jackson2JsonPersonTemplate.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplate.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplate.afterPropertiesSet();

		RedisTemplate<String, byte[]> rawTemplate = new RedisTemplate<String, byte[]>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.setKeySerializer(stringSerializer);
		rawTemplate.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.afterPropertiesSet();

		RedisTemplate genericTemplateLettuce = new RedisTemplate();
		genericTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		genericTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, Person> xGenericTemplateLettuce = new RedisTemplate<String, Person>();
		xGenericTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		xGenericTemplateLettuce.setDefaultSerializer(serializer);
		xGenericTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateLettuce = new RedisTemplate<String, Person>();
		jsonPersonTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		jsonPersonTemplateLettuce.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateLettuce.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateLettuce.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, Person> jackson2JsonPersonTemplateLettuce = new RedisTemplate<String, Person>();
		jackson2JsonPersonTemplateLettuce.setConnectionFactory(lettuceConnFactory);
		jackson2JsonPersonTemplateLettuce.setDefaultSerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLettuce.setHashKeySerializer(jackson2JsonSerializer);
		jackson2JsonPersonTemplateLettuce.setHashValueSerializer(jackson2JsonStringSerializer);
		jackson2JsonPersonTemplateLettuce.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate();
		stringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		stringTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, byte[]> rawTemplateLtc = new RedisTemplate<String, byte[]>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.setKeySerializer(stringSerializer);
		rawTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ personFactory, personFactory, genericTemplate }, { stringFactory, personFactory, genericTemplate },
				{ personFactory, stringFactory, genericTemplate }, { personFactory, stringFactory, xstreamGenericTemplate },
				{ personFactory, stringFactory, jsonPersonTemplate },
				{ personFactory, stringFactory, jackson2JsonPersonTemplate }, { rawFactory, rawFactory, rawTemplate },
				{ stringFactory, stringFactory, genericTemplateLettuce },
				{ personFactory, personFactory, genericTemplateLettuce },
				{ stringFactory, personFactory, genericTemplateLettuce },
				{ personFactory, stringFactory, genericTemplateLettuce },
				{ personFactory, stringFactory, xGenericTemplateLettuce },
				{ personFactory, stringFactory, jsonPersonTemplateLettuce },
				{ personFactory, stringFactory, jackson2JsonPersonTemplateLettuce },
				{ stringFactory, doubleFactory, stringTemplateLtc }, { stringFactory, longFactory, stringTemplateLtc },
				{ rawFactory, rawFactory, rawTemplateLtc } });
	}
}
