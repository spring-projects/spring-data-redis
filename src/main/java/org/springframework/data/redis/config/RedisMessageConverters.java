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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.springframework.messaging.converter.*;
import org.springframework.util.ClassUtils;

/**
 * Represents a builder for {@link MessageConverter}s to be used with Redis listeners.
 *
 * @author Ilyass Bougati
 */
public final class RedisMessageConverters {

	private static final boolean jackson2Present;
	private static final boolean gsonPresent;
	private static final boolean jsonbPresent;
	private static final boolean kotlinSerializationJsonPresent;

	static {
		ClassLoader classLoader = RedisMessageConverters.class.getClassLoader();
		jackson2Present = ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader)
				&& ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);
		gsonPresent = ClassUtils.isPresent("com.google.gson.Gson", classLoader);
		jsonbPresent = ClassUtils.isPresent("jakarta.json.bind.Jsonb", classLoader);
		kotlinSerializationJsonPresent = ClassUtils.isPresent("kotlinx.serialization.json.Json", classLoader);
	}

	private RedisMessageConverters() {}

	public interface Builder {
		Builder registerDefaults(boolean registerDefaults);

		Builder withStringConverter(MessageConverter stringMessageConverter);

		default Builder withStringConverter(Charset charset) {
			return withStringConverter(new StringMessageConverter(charset));
		}

		Builder addCustomConverter(MessageConverter converter);

		MessageConverter build();
	}

	static class DefaultBuilder implements Builder {
		private boolean registerDefaults = true;
		private MessageConverter stringMessageConverter;
		private final List<MessageConverter> customConverters = new ArrayList<>();

		@Override
		public Builder registerDefaults(boolean registerDefaults) {
			this.registerDefaults = registerDefaults;
			return this;
		}

		@Override
		public Builder withStringConverter(MessageConverter stringMessageConverter) {
			this.stringMessageConverter = stringMessageConverter;
			return this;
		}

		@Override
		public Builder addCustomConverter(MessageConverter converter) {
			this.customConverters.add(converter);
			return this;
		}

		@Override
		public MessageConverter build() {
			List<MessageConverter> converters = new ArrayList<>();

			if (this.stringMessageConverter != null) {
				converters.add(this.stringMessageConverter);
			} else if (this.registerDefaults) {
				converters.add(new StringMessageConverter(StandardCharsets.UTF_8));
			}

			converters.addAll(this.customConverters);

			if (this.registerDefaults) {
				converters.add(new ByteArrayMessageConverter());

				if (jackson2Present) {
					converters.add(new JacksonJsonMessageConverter());
				}
				if (gsonPresent) {
					converters.add(new GsonMessageConverter());
				}
				if (jsonbPresent) {
					converters.add(new JsonbMessageConverter());
				}
				if (kotlinSerializationJsonPresent) {
					converters.add(new KotlinSerializationJsonMessageConverter());
				}
			}

			if (converters.isEmpty()) {
				return new StringMessageConverter(StandardCharsets.UTF_8);
			}

			if (converters.size() == 1) {
				return converters.get(0);
			}

			return new CompositeMessageConverter(converters);
		}
	}

	public static Builder builder() {
		return new DefaultBuilder();
	}
}
