/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.support;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.keyvalue.redis.SettingsUtils;
import org.springframework.data.keyvalue.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.keyvalue.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.keyvalue.redis.core.StringRedisTemplate;
import org.springframework.data.keyvalue.redis.support.atomic.RedisAtomicInteger;
import org.springframework.data.keyvalue.redis.support.atomic.RedisAtomicLong;
import org.springframework.data.keyvalue.redis.support.collections.DefaultRedisList;
import org.springframework.data.keyvalue.redis.support.collections.DefaultRedisMap;
import org.springframework.data.keyvalue.redis.support.collections.DefaultRedisSet;
import org.springframework.data.keyvalue.redis.support.collections.RedisList;
import org.springframework.data.keyvalue.redis.support.collections.StringObjectFactory;

/**
 * @author Costin Leau
 */
public class BoundKeyParams {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.afterPropertiesSet();

		// jredis factory
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory();
		jredisConnFactory.setUsePool(true);
		jredisConnFactory.setPort(SettingsUtils.getPort());
		jredisConnFactory.setHostName(SettingsUtils.getHost());
		jredisConnFactory.afterPropertiesSet();

		StringRedisTemplate templateJS = new StringRedisTemplate(jedisConnFactory);
		StringRedisTemplate templateJR = new StringRedisTemplate(jredisConnFactory);

		StringObjectFactory sof = new StringObjectFactory();

		DefaultRedisMap mapJS = new DefaultRedisMap("bound:key:map", templateJS);
		mapJS.put("foo", "bar");

		DefaultRedisSet setJS = new DefaultRedisSet("bound:key:set", templateJS);
		setJS.add("foo");
		
		RedisList list = new DefaultRedisList("bound:key:list", templateJS);
		list.add("foo");

		return Arrays.asList(new Object[][] {
				{ new RedisAtomicInteger("bound:key:int", jedisConnFactory), sof, jedisConnFactory },
				{ new RedisAtomicLong("bound:key:long", jedisConnFactory), sof, jedisConnFactory },
				{ list, sof, jedisConnFactory },
				{ setJS, sof, jedisConnFactory }, { mapJS, sof, jedisConnFactory } });
	}
}
