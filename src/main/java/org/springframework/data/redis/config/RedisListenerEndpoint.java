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

import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * Model representing a Redis listener endpoint.
 * <p>
 * An endpoint describes how to create a listener container (topics, patterns, ID)
 * without managing the lifecycle of the container itself.
 *
 * @author Ilyass Bougati
 * @see RedisListenerContainerFactory
 */
public interface RedisListenerEndpoint {

    /**
     * Return the id of this endpoint.
     */
    String getId();

    /**
     * Setup the specified message listener container with the model defined by this endpoint.
     * <p>This endpoint must provide the requested missing details (topics, etc.)
     * to the specified container.
     *
     * @param listenerContainer the listener container to configure
     */
    void setupListenerContainer(RedisMessageListenerContainer listenerContainer);
}
