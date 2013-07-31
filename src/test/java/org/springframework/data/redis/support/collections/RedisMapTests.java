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
import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jredis.JredisPool;
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * Integration test for RedisMap.
 * 
 * @author Costin Leau
 * @author Jennifer Hickey
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
		StringRedisSerializer stringSerializer = new StringRedisSerializer();

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<String> doubleFactory = new DoubleAsStringObjectFactory();
		ObjectFactory<String> longFactory = new LongAsStringObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

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

		RedisTemplate<String, byte[]> rawTemplate = new RedisTemplate<String, byte[]>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.setKeySerializer(stringSerializer);
		rawTemplate.afterPropertiesSet();

		// JRedis
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory(new JredisPool(
				SettingsUtils.getHost(), SettingsUtils.getPort()));
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

		RedisTemplate<String, byte[]> rawTemplateJR = new RedisTemplate<String, byte[]>();
		rawTemplateJR.setEnableDefaultSerializer(false);
		rawTemplateJR.setConnectionFactory(jredisConnFactory);
		rawTemplateJR.setKeySerializer(stringSerializer);
		rawTemplateJR.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
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

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate();
		stringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		stringTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, byte[]> rawTemplateLtc = new RedisTemplate<String, byte[]>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.setKeySerializer(stringSerializer);
		rawTemplateLtc.afterPropertiesSet();

		// SRP
		SrpConnectionFactory srpConnFactory = new SrpConnectionFactory();
		srpConnFactory.setPort(SettingsUtils.getPort());
		srpConnFactory.setHostName(SettingsUtils.getHost());
		srpConnFactory.afterPropertiesSet();

		RedisTemplate genericTemplateSrp = new RedisTemplate();
		genericTemplateSrp.setConnectionFactory(srpConnFactory);
		genericTemplateSrp.afterPropertiesSet();

		RedisTemplate<String, Person> xGenericTemplateSrp = new RedisTemplate<String, Person>();
		xGenericTemplateSrp.setConnectionFactory(srpConnFactory);
		xGenericTemplateSrp.setDefaultSerializer(serializer);
		xGenericTemplateSrp.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateSrp = new RedisTemplate<String, Person>();
		jsonPersonTemplateSrp.setConnectionFactory(srpConnFactory);
		jsonPersonTemplateSrp.setDefaultSerializer(jsonSerializer);
		jsonPersonTemplateSrp.setHashKeySerializer(jsonSerializer);
		jsonPersonTemplateSrp.setHashValueSerializer(jsonStringSerializer);
		jsonPersonTemplateSrp.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateSrp = new StringRedisTemplate();
		stringTemplateSrp.setConnectionFactory(srpConnFactory);
		stringTemplateSrp.afterPropertiesSet();

		RedisTemplate<String, byte[]> rawTemplateSrp = new RedisTemplate<String, byte[]>();
		rawTemplateSrp.setEnableDefaultSerializer(false);
		rawTemplateSrp.setConnectionFactory(srpConnFactory);
		rawTemplateSrp.setKeySerializer(stringSerializer);
		rawTemplateSrp.afterPropertiesSet();


		return Arrays.asList(new Object[][] {
				{ stringFactory, stringFactory, genericTemplate },
				{ personFactory, personFactory, genericTemplate },
				{ stringFactory, personFactory, genericTemplate },
				{ personFactory, stringFactory, genericTemplate },
				{ personFactory, stringFactory, xstreamGenericTemplate },
				{ personFactory, stringFactory, jsonPersonTemplate },
				{ rawFactory, rawFactory, rawTemplate},
				{ stringFactory, stringFactory, genericTemplateJR },
				{ personFactory, personFactory, genericTemplateJR },
				{ stringFactory, personFactory, genericTemplateJR },
				{ personFactory, stringFactory, genericTemplateJR },
				{ personFactory, stringFactory, xGenericTemplateJR },
				{ personFactory, stringFactory, jsonPersonTemplateJR },
				{ rawFactory, rawFactory, rawTemplateJR},
				{ stringFactory, stringFactory, genericTemplateLettuce },
				{ personFactory, personFactory, genericTemplateLettuce },
				{ stringFactory, personFactory, genericTemplateLettuce },
				{ personFactory, stringFactory, genericTemplateLettuce },
				{ personFactory, stringFactory, xGenericTemplateLettuce },
				{ personFactory, stringFactory, jsonPersonTemplateLettuce },
				{ stringFactory, doubleFactory, stringTemplateLtc},
				{ stringFactory, longFactory, stringTemplateLtc},
				{ rawFactory, rawFactory, rawTemplateLtc},
				{ stringFactory, stringFactory, genericTemplateSrp },
				{ personFactory, personFactory, genericTemplateSrp },
				{ stringFactory, personFactory, genericTemplateSrp },
				{ personFactory, stringFactory, genericTemplateSrp },
				{ personFactory, stringFactory, xGenericTemplateSrp },
				{ stringFactory, doubleFactory, stringTemplateSrp},
				{ stringFactory, longFactory, stringTemplateSrp},
				{ personFactory, stringFactory, jsonPersonTemplateSrp },
				{ rawFactory, rawFactory, rawTemplateSrp}});
	}
}