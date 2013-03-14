/*
 * Copyright 2011-2013 the original author or authors.
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
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.connection.rjc.RjcConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.RedisMap;
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

		// JRedis
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory();
		jredisConnFactory.setUsePool(true);
		jredisConnFactory.setPort(SettingsUtils.getPort());
		jredisConnFactory.setHostName(SettingsUtils.getHost());
		jredisConnFactory.afterPropertiesSet();

		RedisTemplate genericTemplateJR = new RedisTemplate();
		genericTemplateJR.setConnectionFactory(jredisConnFactory);
		genericTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> xGenericTemplateJR = new RedisTemplate<String, Person>();
		xGenericTemplateJR.setConnectionFactory(jredisConnFactory);
		xGenericTemplateJR.setDefaultSerializer(serializer);
		xGenericTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateJR = new RedisTemplate<String, Person>();
		jsonPersonTemplateJR.setConnectionFactory(jredisConnFactory);
		jsonPersonTemplateJR.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateJR.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateJR.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateJR.afterPropertiesSet();

		// RJC

		// rjc
		RjcConnectionFactory rjcConnFactory = new RjcConnectionFactory();
		rjcConnFactory.setUsePool(true);
		rjcConnFactory.setPort(SettingsUtils.getPort());
		rjcConnFactory.setHostName(SettingsUtils.getHost());
		rjcConnFactory.afterPropertiesSet();

		RedisTemplate genericTemplateRJC = new RedisTemplate();
		genericTemplateRJC.setConnectionFactory(rjcConnFactory);
		genericTemplateRJC.afterPropertiesSet();

		RedisTemplate<String, Person> xGenericTemplateRJC = new RedisTemplate<String, Person>();
		xGenericTemplateRJC.setConnectionFactory(rjcConnFactory);
		xGenericTemplateRJC.setDefaultSerializer(serializer);
		xGenericTemplateRJC.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateRJC = new RedisTemplate<String, Person>();
		jsonPersonTemplateRJC.setConnectionFactory(rjcConnFactory);
		jsonPersonTemplateRJC.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateRJC.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateRJC.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateRJC.afterPropertiesSet();


		return Arrays.asList(new Object[][] { { stringFactory, stringFactory, genericTemplate },
				{ personFactory, personFactory, genericTemplate }, { stringFactory, personFactory, genericTemplate },
				{ personFactory, stringFactory, genericTemplate },
				{ personFactory, stringFactory, xstreamGenericTemplate },
				{ stringFactory, stringFactory, genericTemplateJR },
				{ personFactory, personFactory, genericTemplateJR },
				{ stringFactory, personFactory, genericTemplateJR },
				{ personFactory, stringFactory, genericTemplateJR },
				{ personFactory, stringFactory, xGenericTemplateJR },
				{ personFactory, stringFactory, jsonPersonTemplate },
				{ personFactory, stringFactory, jsonPersonTemplateJR },
				{ stringFactory, stringFactory, genericTemplateRJC },
				{ personFactory, personFactory, genericTemplateRJC },
				{ stringFactory, personFactory, genericTemplateRJC },
				{ personFactory, stringFactory, genericTemplateRJC },
				{ personFactory, stringFactory, xGenericTemplateRJC },
				{ personFactory, stringFactory, jsonPersonTemplateRJC } });
	}
}