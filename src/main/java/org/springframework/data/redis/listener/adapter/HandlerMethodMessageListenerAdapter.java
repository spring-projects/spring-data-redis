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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.support.PubSubHeaders;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * An adapter that delegates {@link MessageListener#onMessage} to a target method using Spring Messaging's
 * {@link InvocableHandlerMethod}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 */
public class HandlerMethodMessageListenerAdapter implements MessageListener {

	private final Log logger = LogFactory.getLog(getClass());

	private final InvocableHandlerMethod handlerMethod;
	private String consumes;

	public HandlerMethodMessageListenerAdapter(InvocableHandlerMethod handlerMethod) {
		Assert.notNull(handlerMethod, "InvocableHandlerMethod must not be null");
		this.handlerMethod = handlerMethod;
	}

	public HandlerMethodMessageListenerAdapter(InvocableHandlerMethod handlerMethod, String consumes) {
		this(handlerMethod);
		this.consumes = consumes;
	}

	@Override
	public void onMessage(Message message, byte @Nullable [] pattern) {

		try {
			MessageBuilder<byte[]> builder = MessageBuilder.withPayload(message.getBody());

			builder.setHeader(PubSubHeaders.CHANNEL, ChannelTopic.of(new String(message.getChannel())));

			if (pattern != null) {
				builder.setHeader(PubSubHeaders.TOPIC, PatternTopic.of(new String(pattern)));
				builder.setHeader(PubSubHeaders.PATTERN, PatternTopic.of(new String(pattern)));
			} else {
				builder.setHeader(PubSubHeaders.TOPIC, ChannelTopic.of(new String(message.getChannel())));
			}

			builder.setHeader(MessageHeaders.CONTENT_TYPE, this.consumes);

			this.handlerMethod.invoke(builder.build());
		} catch (Exception e) {
			logger.error("Failed to invoke Redis listener method '%s'".formatted(handlerMethod), e);
		}
	}

	public void setConsumes(String consumes) {
		this.consumes = consumes;
	}
}
