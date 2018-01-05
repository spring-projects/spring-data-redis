/*
 * Copyright 2017-2018 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.resource.ClientResources;

import java.time.Duration;

import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration.LettuceClientConfigurationBuilder;

/**
 * Creates a specific client configuration for Lettuce tests.
 *
 * @author Mark Paluch
 */
public class LettuceTestClientConfiguration {

	/**
	 * @return an initialized {@link LettuceClientConfigurationBuilder} with shutdown timeout and {@link ClientResources}.
	 * @see LettuceTestClientResources#getSharedClientResources()
	 */
	public static LettuceClientConfigurationBuilder builder() {
		return LettuceClientConfiguration.builder().shutdownTimeout(Duration.ZERO)
				.clientResources(LettuceTestClientResources.getSharedClientResources());
	}

	/**
	 * @return a defaulted {@link LettuceClientConfiguration} for testing.
	 */
	public static LettuceClientConfiguration create() {
		return builder().build();
	}
}
