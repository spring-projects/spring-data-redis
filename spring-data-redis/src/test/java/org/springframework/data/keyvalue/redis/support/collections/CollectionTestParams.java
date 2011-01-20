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
package org.springframework.data.keyvalue.redis.support.collections;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.keyvalue.redis.Person;
import org.springframework.data.keyvalue.redis.SettingsUtils;
import org.springframework.data.keyvalue.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.keyvalue.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.keyvalue.redis.core.RedisTemplate;

/**
 * @author Costin Leau
 */
public abstract class CollectionTestParams {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(true);

		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());

		jedisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplate = new RedisTemplate<String, String>(jedisConnFactory);
		RedisTemplate<String, Person> personTemplate = new RedisTemplate<String, Person>(jedisConnFactory);

		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory();
		jredisConnFactory.setUsePool(false);

		jredisConnFactory.setPort(SettingsUtils.getPort());
		jredisConnFactory.setHostName(SettingsUtils.getHost());

		jredisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateJR = new RedisTemplate<String, String>(jredisConnFactory);
		RedisTemplate<String, Person> personTemplateJR = new RedisTemplate<String, Person>(jredisConnFactory);

		return Arrays.asList(new Object[][] { { stringFactory, stringTemplateJR }, { personFactory, personTemplateJR },
				{ stringFactory, stringTemplate },
				{ personFactory, personTemplate } });
	}
}
