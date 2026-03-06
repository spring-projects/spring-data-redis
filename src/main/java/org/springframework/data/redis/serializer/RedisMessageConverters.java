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

import java.nio.charset.Charset;

import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.ContentTypeResolver;
import org.springframework.messaging.converter.GsonMessageConverter;
import org.springframework.messaging.converter.JsonbMessageConverter;
import org.springframework.messaging.converter.KotlinSerializationJsonMessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.util.MimeType;

/**
 * Converter configuration that provides a {@link Builder} to create a composite {@link MessageConverter} for Redis
 * listener endpoints.
 * <p>
 * By default, it registers a {@link StringMessageConverter}, a {@link ByteArrayMessageConverter}, and JSON converters
 * if a supported library (Jackson, Gson, JSON-B, or Kotlin Serialization) is present on the classpath.
 * <p>
 * Converters are provided with {@link org.springframework.messaging.converter.DefaultContentTypeResolver} support, and
 * the default content type can be configured via {@link Builder#defaultMimeType(MimeType)}.
 * <p>
 * <strong>Note:</strong>{@code RedisMessageConverters} uses Spring Data Redis's {@link RedisSerializer}s for JSON
 * serialization. Its JSON serialization format and behavior might slightly differ from Spring Messaging's
 * {@link org.springframework.messaging.converter.JacksonJsonMessageConverter}. If you wish to use Spring Messaging's
 * variant then configure the desired converter through {@link Builder#addCustomConverter(MessageConverter)}.
 *
 * @author Ilyass Bougati
 * @author Mark Paluch
 * @since 4.1
 * @see org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer
 * @see org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer
 * @see StringMessageConverter
 * @see JsonbMessageConverter
 * @see GsonMessageConverter
 * @see KotlinSerializationJsonMessageConverter
 * @see ByteArrayMessageConverter
 * @see SerializerMessageConverter
 */
public interface RedisMessageConverters {

	/**
	 * Create a new {@link Builder} instance.
	 *
	 * @return a new {@link Builder}
	 */
	static Builder builder() {
		return new DefaultRedisMessageConverters.DefaultBuilder();
	}

	/**
	 * Return the configured {@link MessageConverter}.
	 */
	MessageConverter getConverter();

	/**
	 * A builder for configuring a {@link MessageConverter}.
	 */
	interface Builder {

		/**
		 * Configure the default {@link MimeType} that is provieded through
		 * {@link org.springframework.messaging.converter.DefaultContentTypeResolver} to message converters when using
		 * {@link #registerDefaults(boolean) default registrations}.
		 *
		 * @param defaultMimeType the default mime type to use for converters that support content type resolution.
		 * @return this builder.
		 */
		Builder defaultMimeType(MimeType defaultMimeType);

		/**
		 * Configure whether to register default converters.
		 * <p>
		 * Defaults to {@code true}.
		 *
		 * @param registerDefaults whether to register default converters
		 * @return this builder.
		 */
		Builder registerDefaults(boolean registerDefaults);

		/**
		 * Configure the {@link MessageConverter} to use for {@link String} payloads.
		 *
		 * @param stringMessageConverter the converter to use
		 * @return this builder.
		 */
		Builder withStringConverter(MessageConverter stringMessageConverter);

		/**
		 * Configure a {@link StringMessageConverter} with the given {@link Charset}.
		 *
		 * @param charset the charset to use for string conversion
		 * @return this builder.
		 */
		default Builder withStringConverter(Charset charset) {
			return withStringConverter(new StringMessageConverter(charset));
		}

		/**
		 * Add a custom {@link MessageConverter} to the composite converter using the given {@link RedisSerializer} to use
		 * Spring Data Redis' {@link RedisSerializer} for specific serializers such.
		 * {@link org.springframework.data.redis.serializer.JdkSerializationRedisSerializer JDK serialization}.
		 * <p>
		 * Note that generic serializers may support a broad range of objects and {@link SerializerMessageConverter} should
		 * be configured with
		 * {@link org.springframework.messaging.converter.AbstractMessageConverter#setContentTypeResolver(ContentTypeResolver)
		 * content type resolution} support to ensure it only applies to messages (endpoints) with supported content types.
		 *
		 * @param serializer the custom converter to add.
		 * @return this builder.
		 */
		default Builder addCustomConverter(RedisSerializer<?> serializer) {
			if (serializer instanceof JdkSerializationRedisSerializer jdkSerializer) {
				return addCustomConverter(new JdkSerializerMessageConverter(jdkSerializer));
			}
			return addCustomConverter(new SerializerMessageConverter(serializer));
		}

		/**
		 * Add a custom {@link MessageConverter} to the composite converter.
		 *
		 * @param converter the custom converter to add.
		 * @return this builder.
		 */
		Builder addCustomConverter(MessageConverter converter);

		/**
		 * Build the final {@link MessageConverter}.
		 * <p>
		 * If multiple converters are configured or default converters are registered, this returns a
		 * {@link CompositeMessageConverter}.
		 *
		 * @return the constructed {@link RedisMessageConverters}.
		 */
		RedisMessageConverters build();

	}

}
