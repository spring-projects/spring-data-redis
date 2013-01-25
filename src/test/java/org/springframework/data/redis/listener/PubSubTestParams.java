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
package org.springframework.data.redis.listener;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.rjc.RjcConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.support.collections.ObjectFactory;
import org.springframework.data.redis.support.collections.PersonObjectFactory;
import org.springframework.data.redis.support.collections.StringObjectFactory;

/**
 * @author Costin Leau
 */
public class PubSubTestParams {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		ObjectFactory<String> stringFactory = new StringObjectFactory();
		ObjectFactory<Person> personFactory = new PersonObjectFactory();

		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setUsePool(false);
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.setDatabase(2);

		jedisConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplate = new StringRedisTemplate(jedisConnFactory);
		RedisTemplate<String, Person> personTemplate = new RedisTemplate<String, Person>();
		personTemplate.setConnectionFactory(jedisConnFactory);
		personTemplate.afterPropertiesSet();

		// create RJC

		RjcConnectionFactory rjcConnFactory = new RjcConnectionFactory();
		rjcConnFactory.setUsePool(false);
		rjcConnFactory.setPort(SettingsUtils.getPort());
		rjcConnFactory.setHostName(SettingsUtils.getHost());
		rjcConnFactory.afterPropertiesSet();

		RedisTemplate<String, String> stringTemplateRJC = new StringRedisTemplate(rjcConnFactory);
		RedisTemplate<String, Person> personTemplateRJC = new RedisTemplate<String, Person>();
		personTemplateRJC.setConnectionFactory(rjcConnFactory);
		personTemplateRJC.afterPropertiesSet();


		return Arrays.asList(new Object[][] { { stringFactory, stringTemplate }, { personFactory, personTemplate },
				{ stringFactory, stringTemplateRJC }, { personFactory, personTemplateRJC }
		});
	}
}
