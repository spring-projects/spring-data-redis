/*
 * Copyright 2010-2011 the original author or authors.
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
import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * Integration test for RedisMap.
 * 
 * @author Costin Leau
 */
public class RedisMapTests extends AbstractRedisMapTests<Object, Object> {

	public RedisMapTests(ObjectFactory<Object> keyFactory, ObjectFactory<Object> valueFactory, RedisTemplate template) {
		super(keyFactory, valueFactory, template);
	}

	
	RedisMap<Object, Object> createMap() {
		String redisName = getClass().getSimpleName();
		return new DefaultRedisMap<Object, Object>(redisName, template);
	}

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

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(false);
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

		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ personFactory, personFactory, genericTemplate }, { stringFactory, personFactory, genericTemplate },
				{ personFactory, stringFactory, genericTemplate },
				{ personFactory, stringFactory, xstreamGenericTemplate },
				{ personFactory, stringFactory, jsonPersonTemplate } });
	}
}