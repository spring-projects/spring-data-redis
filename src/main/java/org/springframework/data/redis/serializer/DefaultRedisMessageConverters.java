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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.messaging.converter.AbstractMessageConverter;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.DefaultContentTypeResolver;
import org.springframework.messaging.converter.GsonMessageConverter;
import org.springframework.messaging.converter.JsonbMessageConverter;
import org.springframework.messaging.converter.KotlinSerializationJsonMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.util.ClassUtils;
import org.springframework.util.MimeType;

/**
 * Utility class that provides a {@link Builder} to create a composite {@link MessageConverter} for Redis listener
 * endpoints.
 * <p>
 * By default, it registers a {@link StringMessageConverter}, a {@link ByteArrayMessageConverter}, and a JSON converter
 * if a supported library (Jackson, Gson, JSON-B, or Kotlin Serialization) is present on the classpath.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 */
class DefaultRedisMessageConverters implements RedisMessageConverters {

	private static final MimeType[] JSON_MIME_TYPES = new MimeType[] { new MimeType("application", "json"),
			new MimeType("application", "*+json") };

	private static final boolean JACKSON_PRESENT;

	private static final boolean JACKSON_2_PRESENT;

	private static final boolean GSON_PRESENT;

	private static final boolean JSONB_PRESENT;

	private static final boolean KOTLIN_SERIALIZATION_JSON_PRESENT;

	static {
		ClassLoader classLoader = DefaultRedisMessageConverters.class.getClassLoader();
		JACKSON_PRESENT = ClassUtils.isPresent("tools.jackson.databind.ObjectMapper", classLoader);
		JACKSON_2_PRESENT = ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader)
				&& ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);
		GSON_PRESENT = ClassUtils.isPresent("com.google.gson.Gson", classLoader);
		JSONB_PRESENT = ClassUtils.isPresent("jakarta.json.bind.Jsonb", classLoader);
		KOTLIN_SERIALIZATION_JSON_PRESENT = ClassUtils.isPresent("kotlinx.serialization.json.Json", classLoader);
	}

	private final MessageConverter messageConverter;

	public DefaultRedisMessageConverters(MessageConverter messageConverter) {
		this.messageConverter = messageConverter;
	}

	@Override
	public MessageConverter getConverter() {
		return messageConverter;
	}

	static class DefaultBuilder implements RedisMessageConverters.Builder {

		private @Nullable MimeType defaultMimeType = null;

		private boolean registerDefaults = true;

		private @Nullable MessageConverter stringMessageConverter;

		private final List<MessageConverter> customConverters = new ArrayList<>();

		@Override
		public RedisMessageConverters.Builder defaultMimeType(MimeType defaultMimeType) {
			this.defaultMimeType = defaultMimeType;
			return this;
		}

		@Override
		public RedisMessageConverters.Builder registerDefaults(boolean registerDefaults) {
			this.registerDefaults = registerDefaults;
			return this;
		}

		@Override
		public RedisMessageConverters.Builder withStringConverter(MessageConverter stringMessageConverter) {
			this.stringMessageConverter = stringMessageConverter;
			return this;
		}

		@Override
		public RedisMessageConverters.Builder addCustomConverter(MessageConverter converter) {
			this.customConverters.add(converter);
			return this;
		}

		@Override
		public RedisMessageConverters build() {

			List<MessageConverter> converters = new ArrayList<>();

			if (this.stringMessageConverter != null) {
				converters.add(this.stringMessageConverter);
			} else if (this.registerDefaults) {
				converters.add(new StringMessageConverter(StandardCharsets.UTF_8));
			}

			converters.addAll(this.customConverters);

			if (this.registerDefaults) {
				converters.addAll(getDefaultConverters());
			}

			return new DefaultRedisMessageConverters(new CompositeMessageConverter(converters));
		}

		private List<MessageConverter> getDefaultConverters() {

			List<MessageConverter> defaultConverters = new ArrayList<>();

			if (JACKSON_PRESENT) {
				defaultConverters
						.add(new SerializerMessageConverter(GenericJacksonJsonRedisSerializer.builder().build(), JSON_MIME_TYPES));
			} else if (JACKSON_2_PRESENT) {
				defaultConverters
						.add(new SerializerMessageConverter(
								GenericJackson2JsonRedisSerializer.builder().defaultTyping(false).build(), JSON_MIME_TYPES));
			}
			if (GSON_PRESENT) {
				defaultConverters.add(new GsonMessageConverter());
			}
			if (JSONB_PRESENT) {
				defaultConverters.add(new JsonbMessageConverter());
			}
			if (KOTLIN_SERIALIZATION_JSON_PRESENT) {
				defaultConverters.add(new KotlinSerializationJsonMessageConverter());
			}

			defaultConverters.add(new ByteArrayMessageConverter());

			DefaultContentTypeResolver contentTypeResolver = new DefaultContentTypeResolver();
			contentTypeResolver.setDefaultMimeType(defaultMimeType);

			for (MessageConverter defaultConverter : defaultConverters) {
				if (defaultConverter instanceof AbstractMessageConverter amc) {
					amc.setContentTypeResolver(contentTypeResolver);
				}
			}

			return defaultConverters;
		}

	}

}
