/*
 * Copyright 2014 the original author or authors.
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

import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;

import redis.clients.jedis.Jedis;

/**
 * @author Christoph Strobl
 */
public class RedisSentinelRule implements TestRule {

	enum VerificationMode {
		ALL_ACTIVE, ONE_ACTIVE, NO_SENTINEL
	}

	private static final RedisSentinelConfiguration DEFAULT_SENTINEL_CONFIG = new RedisSentinelConfiguration()
			.master("mymaster").sentinel("127.0.0.1", 26379).sentinel("127.0.0.1", 26380).sentinel("127.0.0.1", 26381);

	private RedisSentinelConfiguration sentinelConfig;
	private VerificationMode mode;

	protected RedisSentinelRule(RedisSentinelConfiguration config) {
		this(config, VerificationMode.ALL_ACTIVE);
	}
	
	protected RedisSentinelRule(RedisSentinelConfiguration config, VerificationMode mode) {
		
		this.sentinelConfig = config;
		this.mode = mode;
	}

	/**
	 * Create new {@link RedisSentinelRule} for given {@link RedisSentinelConfiguration}.
	 * 
	 * @param config
	 * @return
	 */
	public static RedisSentinelRule forConfig(RedisSentinelConfiguration config) {
		return new RedisSentinelRule(config != null ? config : DEFAULT_SENTINEL_CONFIG);
	}

	/**
	 * Create new {@link RedisSentinelRule} using default configuration.
	 * 
	 * @return
	 */
	public static RedisSentinelRule withDefaultConfig() {
		return new RedisSentinelRule(DEFAULT_SENTINEL_CONFIG);
	}

	public RedisSentinelRule sentinelsDisabled() {
		
		this.mode = VerificationMode.NO_SENTINEL;
		return this;
	}

	/**
	 * Verifies all {@literal Sentinel} nodes are available.
	 * 
	 * @return
	 */
	public RedisSentinelRule allActive() {
		
		this.mode = VerificationMode.ALL_ACTIVE;
		return this;
	}

	/**
	 * Verifies at least one {@literal Sentinel} node is available.
	 * 
	 * @return
	 */
	public RedisSentinelRule oneActive() {
		
		this.mode = VerificationMode.ONE_ACTIVE;
		return this;
	}

	@Override
	public Statement apply(final Statement base, Description description) {

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				verify();
				base.evaluate();
			}
		};
	}

	private void verify() {

		int failed = 0;
		for (RedisNode node : sentinelConfig.getSentinels()) {
			if (!isAvailable(node)) {
				failed++;
			}
		}

		if (failed > 0) {
			if (VerificationMode.ALL_ACTIVE.equals(mode)) {
				throw new AssumptionViolatedException(String.format(
						"Expected all Redis Sentinels to respone but %s of %s did not responde", failed, sentinelConfig
								.getSentinels().size()));
			}

			if (VerificationMode.ONE_ACTIVE.equals(mode) && sentinelConfig.getSentinels().size() - 1 < failed) {
				throw new AssumptionViolatedException(
						"Expected at least one sentinel to respond but it seems all are offline - Game Over!");
			}
		}

		if (VerificationMode.NO_SENTINEL.equals(mode) && failed != sentinelConfig.getSentinels().size()) {
			throw new AssumptionViolatedException(String.format(
					"Expected to have no sentinels online but found that %s are still alive.", (sentinelConfig.getSentinels()
							.size() - failed)));
		}
	}

	private boolean isAvailable(RedisNode node) {
		
		Jedis jedis = null;
		try {
			
			jedis = new Jedis(node.getHost(), node.getPort());
			jedis.connect();
			jedis.ping();
		} catch (Exception e) {
			
			return false;
		} finally {
		
			if (jedis != null) {
				try {
			
					jedis.disconnect();
					jedis.close();
				} catch (Exception e) {
					
					e.printStackTrace();
				}
			}
		}
		
		return true;
	}
}
