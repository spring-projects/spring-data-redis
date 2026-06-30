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
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.util.Assert;

/**
 * Implementation of {@link RedisJsonSerializer} with Jackson 3.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public class JacksonRedisJsonSerializer implements RedisJsonSerializer {

	private final JsonMapper mapper;

	public JacksonRedisJsonSerializer(JsonMapper mapper) {
		Assert.notNull(mapper, "JsonMapper must not be null");
		this.mapper = mapper;
	}

	public static JacksonRedisJsonSerializer createDefault() {
		return new JacksonRedisJsonSerializer(JsonMapper.shared());
	}

	@Override
	public String serialize(@Nullable Object value) throws SerializationException {

		if (value == null) {
			return "null";
		}

		try {
			return mapper.writeValueAsString(value);
		} catch (JacksonException ex) {
			throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
		}
	}

	@Override
	public <T> T deserialize(String rawJson, Class<T> type) throws SerializationException {
		try {
			return mapper.readValue(rawJson, type);
		} catch (JacksonException ex) {
			throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
		}
	}

	@Override
	public <T> T deserialize(String rawJson, ParameterizedTypeReference<T> typeRef) throws SerializationException {
		try {
			return mapper.readValue(rawJson, mapper.getTypeFactory().constructType(typeRef.getType()));
		} catch (JacksonException ex) {
			throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
		}
	}

}
