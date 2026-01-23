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
package org.springframework.data.redis.listener.adapter;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * An adapter that delegates {@link MessageListener#onMessage} to a target method using Spring Messaging's
 * {@link InvocableHandlerMethod}.
 *
 * @author Ilyass Bougati
 */
public class MessagingMessageListenerAdapter implements MessageListener {

	private final InvocableHandlerMethod handlerMethod;

	public MessagingMessageListenerAdapter(InvocableHandlerMethod handlerMethod) {
		Assert.notNull(handlerMethod, "InvocableHandlerMethod must not be null");
		this.handlerMethod = handlerMethod;
	}

	@Override
	public void onMessage(Message message, byte @Nullable [] pattern) {
		try {
			org.springframework.messaging.Message<byte[]> springMessage = MessageBuilder.withPayload(message.getBody())
					.setHeader("redis_channel", message.getChannel()).setHeader("redis_pattern", pattern)
					.setHeader("redis_raw_message", message).build();

			this.handlerMethod.invoke(springMessage);
		} catch (Exception e) {
			// TODO: Integrate with @RedisExceptionHandler later as discussed with mp911de
			throw new RuntimeException("Failed to invoke Redis listener method", e);
		}
	}
}
