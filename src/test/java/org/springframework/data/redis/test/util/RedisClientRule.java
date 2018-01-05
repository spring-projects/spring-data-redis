/*
 * Copyright 2014-2018 the original author or authors.
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

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * A JUnit {@link TestRule} that checks whether a given {@link RedisConnectionFactory} is from the required
 * {@link RedisDriver}.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
public abstract class RedisClientRule implements TestRule {

	@Override
	public Statement apply(final Statement base, final Description description) {

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				WithRedisDriver withRedisDriver = description.getAnnotation(WithRedisDriver.class);
				RedisConnectionFactory redisConnectionFactory = getConnectionFactory();

				if (withRedisDriver != null && redisConnectionFactory != null) {

					for (RedisDriver driver : withRedisDriver.value()) {

						if (driver.matches(redisConnectionFactory)) {
							base.evaluate();
							return;
						}
					}
					throw new AssumptionViolatedException("not a vaild redis connection for driver: " + redisConnectionFactory);
				}

				base.evaluate();
			}
		};
	}

	public abstract RedisConnectionFactory getConnectionFactory();
}
