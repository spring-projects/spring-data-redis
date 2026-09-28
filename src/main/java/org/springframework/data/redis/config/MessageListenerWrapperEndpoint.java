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

import org.jspecify.annotations.Nullable;

import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

/**
 * @author Mark Paluch
 */
public class MessageListenerWrapperEndpoint extends RedisListenerEndpointSupport {

	private final RedisListenerEndpointSupport delegate;

	public MessageListenerWrapperEndpoint(RedisListenerEndpointSupport delegate) {
		this.delegate = delegate;
	}

	@Override
	public String getId() {
		return delegate.getId();
	}

	@Override
	protected @Nullable MessageListener createListener() {

		MessageListener listener = delegate.createListener();
		return (message, pattern) -> listener.onMessage(message, pattern);
	}

	@Override
	protected void subscribe(RedisMessageListenerContainer listenerContainer, MessageListener messageListener) {
		delegate.subscribe(listenerContainer, messageListener);
	}

	@Override
	protected StringBuilder getEndpointDescription() {
		return delegate.getEndpointDescription();
	}

}
