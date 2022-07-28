/*
 * Copyright 2022-2022 the original author or authors.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.springframework.lang.Nullable;

/**
 * <p>{@link RedisSerializer} that can serialize and deserialize objects that contain generics.</p>
 * <p>Example usage for {@literal Set<Person>}:</p>
 * <pre>{@code
 * JacksonTypeReference2JsonRedisSerializer<Set<Person>> personSetSerializer =
 *     new JacksonTypeReference2JsonRedisSerializer<>(new TypeReference<>() {});
 * }
 * </pre>
 * <b>Note:</b>Null objects are serialized as empty arrays and vice versa.
 *
 * @author Jos Roseboom
 * @since 3.0
 */
public class JacksonTypeReference2JsonRedisSerializer<T> implements RedisSerializer<T> {
    private final TypeReference<T> typeReference;
    private final ObjectMapper objectMapper;

    public JacksonTypeReference2JsonRedisSerializer(TypeReference<T> typeReference, ObjectMapper objectMapper) {
        this.typeReference = typeReference;
        this.objectMapper = objectMapper;
    }

    public JacksonTypeReference2JsonRedisSerializer(TypeReference<T> typeReference) {
        this(typeReference, JsonMapper.builder().findAndAddModules().build());
    }

    @Override
    public byte[] serialize(@Nullable Object t) throws SerializationException {

        if (t == null) {
            return SerializationUtils.EMPTY_ARRAY;
        }
        try {
            return this.objectMapper.writeValueAsBytes(t);
        } catch (Exception ex) {
            throw new SerializationException("Could not write JSON: " + ex.getMessage(), ex);
        }
    }

    @Override
    public T deserialize(@Nullable byte[] bytes) throws SerializationException {
        if (SerializationUtils.isEmpty(bytes)) {
            return null;
        }
        try {
            return this.objectMapper.readValue(bytes, typeReference);
        } catch (Exception ex) {
            throw new SerializationException("Could not read JSON: " + ex.getMessage(), ex);
        }
    }
}
