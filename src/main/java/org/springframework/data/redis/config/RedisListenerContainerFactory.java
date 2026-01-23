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

public interface RedisListenerContainerFactory <C extends RedisMessageListenerContainer> {

    /**
     * Create a {@link RedisMessageListenerContainer} for the given {@link RedisListenerEndpoint}.
     *
     * @param endpoint the endpoint to configure
     * @return the created container
     */
    C createListenerContainer(RedisListenerEndpoint endpoint);
}