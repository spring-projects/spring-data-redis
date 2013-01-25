/*
 * Copyright 2010-2011-2013 the original author or authors.
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
package org.springframework.data.redis.support.atomic;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

/**
 * @author Costin Leau
 */
public abstract class AtomicCountersParam {

	public static Collection<Object[]> testParams() {
		// create Jedis Factory
		JedisConnectionFactory jedisConnFactory = new JedisConnectionFactory();
		jedisConnFactory.setPort(SettingsUtils.getPort());
		jedisConnFactory.setHostName(SettingsUtils.getHost());
		jedisConnFactory.setUsePool(false);
		jedisConnFactory.afterPropertiesSet();

		// jredis factory
		//		JredisConnectionFactory jredisConnFactory = new JredisConnectionFactory();
		//		jredisConnFactory.setUsePool(true);
		//		jredisConnFactory.setPort(SettingsUtils.getPort());
		//		jredisConnFactory.setHostName(SettingsUtils.getHost());
		//		jredisConnFactory.afterPropertiesSet();

		return Arrays.asList(new Object[][] { { jedisConnFactory } });
	}
}
