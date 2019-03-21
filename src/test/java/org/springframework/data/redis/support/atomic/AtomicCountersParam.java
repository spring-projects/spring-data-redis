/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.support.atomic;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jredis.JredisPool;
import org.springframework.data.redis.connection.jredis.JredisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.srp.SrpConnectionFactory;

/**
 * @author Costin Leau
 * @author Jennifer Hickey
 */
public abstract class AtomicCountersParam {

	public static Collection<Object[]> testParams() {
		// Jedis
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.setUsePool(true);
		jedisConnFactory.afterPropertiesSet();

		// JRedis
		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory(new JredisPool(SettingsUtils.getHost(),
				SettingsUtils.getPort()));
		jredisConnFactory.afterPropertiesSet();

		// Lettuce
		LettuceConnectionFactory lettuceConnFactory = new LettuceConnectionFactory();
		lettuceConnFactory.setPort(SettingsUtils.getPort());
		lettuceConnFactory.setHostName(SettingsUtils.getHost());
		lettuceConnFactory.afterPropertiesSet();

		// SRP
		SrpConnectionFactory srpConnFactory = new SrpConnectionFactory();
		srpConnFactory.setPort(SettingsUtils.getPort());
		srpConnFactory.setHostName(SettingsUtils.getHost());
		srpConnFactory.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { jedisConnFactory }, { jredisConnFactory }, { lettuceConnFactory },
				{ srpConnFactory } });
	}
}
