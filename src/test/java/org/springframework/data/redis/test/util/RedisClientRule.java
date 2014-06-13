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
import org.springframework.data.redis.connection.ConnectionUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * @author Christoph Strobl
 */
public class RedisClientRule implements TestRule {

	boolean valid = false;
	private RedisConnectionFactory redisConnectionFactory;

	public static RedisClientRule none() {
		RedisClientRule rule = new RedisClientRule();
		rule.valid = true;
		return rule;
	}

	public static RedisClientRule appliedOn(RedisConnectionFactory connectionFactory) {
		RedisClientRule rule = new RedisClientRule();
		rule.redisConnectionFactory = connectionFactory;
		return rule;
	}

	@Override
	public Statement apply(final Statement base, Description description) {
		return new Statement() {

			@Override
			public void evaluate() throws Throwable {
				RedisClientRule.this.evaluate();
				base.evaluate();
			}
		};
	}

	public RedisClientRule jedis() {
		if (!valid && ConnectionUtils.isJedis(redisConnectionFactory)) {
			valid = true;
		}
		return this;
	}

	public RedisClientRule jredis() {
		if (!valid && ConnectionUtils.isJedis(redisConnectionFactory)) {
			valid = true;
		}
		return this;
	}

	public RedisClientRule lettuce() {
		if (!valid && ConnectionUtils.isLettuce(redisConnectionFactory)) {
			valid = true;
		}
		return this;
	}

	public RedisClientRule srp() {
		if (!valid && ConnectionUtils.isSrp(redisConnectionFactory)) {
			valid = true;
		}
		return this;
	}

	public void evaluate() {
		if (!valid) {
			throw new AssumptionViolatedException("not a vaild redis connection");
		}
	}

}
