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

import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.MimeType;

/**
 * {@link MessageConverter} adapter for {@link JdkSerializationRedisSerializer}. Useful to inherit supported mimetype
 * defaults.
 *
 * @author Mark Paluch
 * @since 4.1
 */
public class JdkSerializerMessageConverter extends SerializerMessageConverter implements MessageConverter {

	/**
	 * A String equivalent of {@link #APPLICATION_JAVA_SERIALIZED_OBJECT}.
	 */
	public static final String APPLICATION_JAVA_SERIALIZED_OBJECT_VALUE = "application/java-serialized-object";

	/**
	 * Media type for {@code application/java-serialized-object}.
	 */
	public static final MimeType APPLICATION_JAVA_SERIALIZED_OBJECT = MimeType
			.valueOf(APPLICATION_JAVA_SERIALIZED_OBJECT_VALUE);

	/**
	 * Create a new {@code JdkSerializerMessageConverter} given {@link ClassLoader}.
	 *
	 * @param classLoader the class loader to use for deserialization; can be {@literal null} to use the default class
	 *          loader.
	 */
	public JdkSerializerMessageConverter(@Nullable ClassLoader classLoader) {
		this(new JdkSerializationRedisSerializer(classLoader));
	}

	/**
	 * Create a new {@code JdkSerializerMessageConverter} given {@link JdkSerializationRedisSerializer}.
	 *
	 * @param serializer the serializer to use.
	 */
	public JdkSerializerMessageConverter(JdkSerializationRedisSerializer serializer) {
		super(serializer);
		this.addSupportedMimeTypes(APPLICATION_JAVA_SERIALIZED_OBJECT);
	}

}
