/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.test.util;

import static org.hamcrest.CoreMatchers.*;

import redis.clients.jedis.Jedis;

import org.junit.Assume;
import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.jedis.JedisConverters;

/**
 * Simple {@link TestRule} implementation that check Redis is running in cluster mode.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisClusterRule extends ExternalResource {

	private RedisClusterConfiguration clusterConfig;
	private String mode;

	/**
	 * Create and init {@link RedisClusterRule} with default configuration ({@code host=127.0.0.1 port=7379}).
	 */
	public RedisClusterRule() {
		this(new RedisClusterConfiguration().clusterNode("127.0.0.1", 7379));
	}

	/**
	 * Create and init {@link RedisClientRule} with given configuration.
	 *
	 * @param config
	 */
	public RedisClusterRule(RedisClusterConfiguration config) {
		this.clusterConfig = config;
		init();
	}

	/*
	 * (non-Javadoc)
	 * @see org.junit.rules.ExternalResource#before()
	 */
	@Override
	public void before() {
		Assume.assumeThat(mode, is("cluster"));
	}

	private void init() {

		if (clusterConfig == null) {
			return;
		}

		for (RedisNode node : clusterConfig.getClusterNodes()) {

			Jedis jedis = null;
			try {

				jedis = new Jedis(node.getHost(), node.getPort());
				mode = JedisConverters.toProperties(jedis.info()).getProperty("redis_mode");
				return;
			} catch (Exception e) {
				// ignore and move on
			} finally {
				jedis.close();
			}
		}
	}
}
