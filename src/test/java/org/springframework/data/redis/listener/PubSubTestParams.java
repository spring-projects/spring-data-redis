/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.redis.listener;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.PersonObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.StringObjectFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Mark Paluch
 */
public class PubSubTestParams {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();
		ObjectFactory<byte[]> rawFactory = new RawObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(true);
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.setDatabase(2);

		jedisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate(jedisConnFactory);
		RedisTemplate<String, Person> personTemplate = new RedisTemplate<String, Person>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();
		RedisTemplate<byte[], byte[]> rawTemplate = new RedisTemplate<byte[], byte[]>();
		rawTemplate.setEnableDefaultSerializer(false);
		rawTemplate.setConnectionFactory(jedisConnFactory);
		rawTemplate.afterPropertiesSet();

		// add Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
		lettuceConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateLtc = new StringRedisTemplate(lettuceConnFactory);
		RedisTemplate<String, Person> personTemplateLtc = new RedisTemplate<String, Person>();
		personTemplateLtc.setConnectionFactory(lettuceConnFactory);
		personTemplateLtc.afterPropertiesSet();
		RedisTemplate<byte[], byte[]> rawTemplateLtc = new RedisTemplate<byte[], byte[]>();
		rawTemplateLtc.setEnableDefaultSerializer(false);
		rawTemplateLtc.setConnectionFactory(lettuceConnFactory);
		rawTemplateLtc.afterPropertiesSet();

		// SRP
		SrpConnectionFactory srpConnFactory = new SrpConnectionFactory();
		srpConnFactory.setPort(SettingsUtils.getPort());
		srpConnFactory.setHostName(SettingsUtils.getHost());
		srpConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateSrp = new StringRedisTemplate(srpConnFactory);
		RedisTemplate<String, Person> personTemplateSrp = new RedisTemplate<String, Person>();
		personTemplateSrp.setConnectionFactory(srpConnFactory);
		personTemplateSrp.afterPropertiesSet();
		RedisTemplate<byte[], byte[]> rawTemplateSrp = new RedisTemplate<byte[], byte[]>();
		rawTemplateSrp.setEnableDefaultSerializer(false);
		rawTemplateSrp.setConnectionFactory(srpConnFactory);
		rawTemplateSrp.afterPropertiesSet();

		// JRedis does not support pub/sub

		return Arrays.asList(new Object[][] { { stringFactory, stringTemplate }, { personFactory, personTemplate },
				{ rawFactory, rawTemplate }, { stringFactory, stringTemplateLtc }, { personFactory, personTemplateLtc },
				{ rawFactory, rawTemplateLtc }, { stringFactory, stringTemplateSrp }, { personFactory, personTemplateSrp },
				{ rawFactory, rawTemplateSrp } });
	}
}
