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
package org.springframework.data.redis.serializer;

import org.jspecify.annotations.Nullable;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;

/**
 * {@link MessageConverter} adapter for {@link RedisSerializer}.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public class SerializerMessageConverter extends AbstractMessageConverter implements MessageConverter {

	private final RedisSerializer<Object> serializer;

	public SerializerMessageConverter(RedisSerializer<?> serializer) {
		this.serializer = (RedisSerializer<Object>) serializer;
	}

	public SerializerMessageConverter(RedisSerializer<?> serializer, MimeType... supportedMimeTypes) {
		this(serializer);
		addSupportedMimeTypes(supportedMimeTypes);
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return serializer.canSerialize(clazz);
	}

	@Override
	protected @Nullable Object convertFromInternal(Message<?> message, Class<?> targetClass,
			@Nullable Object conversionHint) {

		if (serializer.canSerialize(targetClass)) {

			Object payload = message.getPayload();
			if (payload instanceof byte[] bytes) {
				return serializer.deserialize(bytes, targetClass);
			}
		}

		return null;
	}

	@Override
	protected @Nullable Object convertToInternal(Object payload, @Nullable MessageHeaders headers,
			@Nullable Object conversionHint) {
		return serializer.canSerialize(payload.getClass()) ? serializer.serialize(payload) : null;
	}

}
