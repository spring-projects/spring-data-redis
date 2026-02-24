/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.config;

/**
 * Configuration constants for internal sharing across subpackages.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public abstract class RedisListenerConfigUtils {

	/**
	 * The bean name of the internally managed Redis listener annotation processor.
	 */
	public static final String REDIS_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME = "org.springframework.data.redis.config.internalRedisListenerAnnotationProcessor";

	/**
	 * The bean name of the internally managed Redis listener endpoint registry.
	 */
	public static final String REDIS_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME = "org.springframework.data.redis.config.internalRedisListenerEndpointRegistry";

}
