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
package org.springframework.data.redis.annotation;

/**
 * Strategy for registering {@link RedisListener @RedisListener} methods with a
 * {@link org.springframework.data.redis.listener.RedisMessageListenerContainer}.
 *
 * @author Moritz Halbritter
 * @since 4.2
 * @see EnableRedisListeners#grouping()
 */
public enum ListenerGrouping {

	/**
	 * Register one listener per {@link RedisListener @RedisListener} annotation. Each listener receives its own
	 * subscription callbacks and messages are dispatched to each method independently. For example, three methods
	 * listening on {@code ch1} result in three listeners and three tasks per message.
	 */
	PER_ANNOTATION,

	/**
	 * Register one listener per bean, container and topic. Methods of the same bean listening to the same topic are
	 * invoked sequentially for each message, in no particular order. Subscription callbacks are delivered once per
	 * bean. For example, three methods of one bean listening on {@code ch1} result in one listener and one task per
	 * message.
	 * <p>
	 * Of each grouped endpoint, only the listener it creates is used; lifecycle is managed for the group as a whole. See
	 * {@link org.springframework.data.redis.config.BatchMethodRedisListenerEndpoint} for details.
	 */
	PER_BEAN_AND_TOPIC

}
