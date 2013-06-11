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

import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.JacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.OxmSerializer;
import org.springframework.oxm.xstream.XStreamMarshaller;

/**
 * @author Costin Leau
 */
public abstract class CollectionTestParams {

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

		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(true);

		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());

		jedisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate(jedisConnFactory);
		RedisTemplate<String, Person> personTemplate = new RedisTemplate<String, Person>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();

		RedisTemplate<String, String> xstreamStringTemplate = new RedisTemplate<String, String>();
		xstreamStringTemplate.setConnectionFactory(jedisConnFactory);
		xstreamStringTemplate.setDefaultSerializer(serializer);
		xstreamStringTemplate.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplate = new RedisTemplate<String, Person>();
		xstreamPersonTemplate.setConnectionFactory(jedisConnFactory);
		xstreamPersonTemplate.setValueSerializer(serializer);
		xstreamPersonTemplate.afterPropertiesSet();

		// json
		RedisTemplate<String, Person> jsonPersonTemplate = new RedisTemplate<String, Person>();
		jsonPersonTemplate.setConnectionFactory(jedisConnFactory);
		jsonPersonTemplate.setValueSerializer(jsonSerializer);
		jsonPersonTemplate.afterPropertiesSet();

		// jredis
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory();

		jredisConnFactory.setPort(SettingsUtils.getPort());
		jredisConnFactory.setHostName(SettingsUtils.getHost());

		jredisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateJR = new StringRedisTemplate(jredisConnFactory);
		RedisTemplate<String, Person> personTemplateJR = new RedisTemplate<String, Person>();
		personTemplateJR.setConnectionFactory(jredisConnFactory);
		personTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamStringTemplateJR = new RedisTemplate<String, Person>();
		xstreamStringTemplateJR.setConnectionFactory(jredisConnFactory);
		xstreamStringTemplateJR.setDefaultSerializer(serializer);
		xstreamStringTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplateJR = new RedisTemplate<String, Person>();
		xstreamPersonTemplateJR.setValueSerializer(serializer);
		xstreamPersonTemplateJR.setConnectionFactory(jredisConnFactory);
		xstreamPersonTemplateJR.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateJR = new RedisTemplate<String, Person>();
		jsonPersonTemplateJR.setValueSerializer(jsonSerializer);
		jsonPersonTemplateJR.setConnectionFactory(jredisConnFactory);
		jsonPersonTemplateJR.afterPropertiesSet();

		// SRP
		SrpConnectionFactory srConnFactory = new SrpConnectionFactory();
		srConnFactory.setPort(SettingsUtils.getPort());
		srConnFactory.setHostName(SettingsUtils.getHost());
		srConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateSRP = new StringRedisTemplate(srConnFactory);
		RedisTemplate<String, Person> personTemplateSRP = new RedisTemplate<String, Person>();
		personTemplateSRP.setConnectionFactory(srConnFactory);
		personTemplateSRP.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamStringTemplateSRP = new RedisTemplate<String, Person>();
		xstreamStringTemplateSRP.setConnectionFactory(srConnFactory);
		xstreamStringTemplateSRP.setDefaultSerializer(serializer);
		xstreamStringTemplateSRP.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplateSRP = new RedisTemplate<String, Person>();
		xstreamPersonTemplateSRP.setValueSerializer(serializer);
		xstreamPersonTemplateSRP.setConnectionFactory(srConnFactory);
		xstreamPersonTemplateSRP.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateSRP = new RedisTemplate<String, Person>();
		jsonPersonTemplateSRP.setValueSerializer(jsonSerializer);
		jsonPersonTemplateSRP.setConnectionFactory(srConnFactory);
		jsonPersonTemplateSRP.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
		lettuceConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate(lettuceConnFactory);
		RedisTemplate<String, Person> personTemplateLtc = new RedisTemplate<String, Person>();
		personTemplateLtc.setConnectionFactory(lettuceConnFactory);
		personTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamStringTemplateLtc = new RedisTemplate<String, Person>();
		xstreamStringTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xstreamStringTemplateLtc.setDefaultSerializer(serializer);
		xstreamStringTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> xstreamPersonTemplateLtc = new RedisTemplate<String, Person>();
		xstreamPersonTemplateLtc.setValueSerializer(serializer);
		xstreamPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		xstreamPersonTemplateLtc.afterPropertiesSet();

		RedisTemplate<String, Person> jsonPersonTemplateLtc = new RedisTemplate<String, Person>();
		jsonPersonTemplateLtc.setValueSerializer(jsonSerializer);
		jsonPersonTemplateLtc.setConnectionFactory(lettuceConnFactory);
		jsonPersonTemplateLtc.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { stringFactory, stringTemplate },
				//{ stringFactory, stringTemplateJR },
				//{ personFactory, personTemplateJR }, 
				{ personFactory, personTemplate },
				{ stringFactory, xstreamStringTemplate }, { personFactory, xstreamPersonTemplate },
				//{ stringFactory, xstreamStringTemplateJR }, 
				//{ personFactory, xstreamPersonTemplateJR },
				{ personFactory, jsonPersonTemplate },
				//{ personFactory, jsonPersonTemplateJR },
				// srp
				{ stringFactory, stringTemplateSRP },{ personFactory, personTemplateSRP },
				{ stringFactory, xstreamStringTemplateSRP }, { personFactory, xstreamPersonTemplateSRP },
				{ personFactory, jsonPersonTemplateSRP },
				// lettuce
				{ stringFactory, stringTemplateLtc }, { personFactory, personTemplateLtc },
				{ stringFactory, xstreamStringTemplateLtc }, { personFactory, xstreamPersonTemplateLtc },
				{ personFactory, jsonPersonTemplateLtc }

		});
	}
}
