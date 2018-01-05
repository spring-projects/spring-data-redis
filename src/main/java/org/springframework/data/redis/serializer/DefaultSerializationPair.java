/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.serializer;

import lombok.Getter;

import org.springframework.data.redis.serializer.RedisSerializationContext.SerializationPair;

/**
 * Default implementation of {@link SerializationPair}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@Getter
class DefaultSerializationPair<T> implements SerializationPair<T> {

	private final RedisElementReader<T> reader;
	private final RedisElementWriter<T> writer;

	@SuppressWarnings("unchecked")
	DefaultSerializationPair(RedisElementReader<? extends T> reader, RedisElementWriter<? extends T> writer) {

		this.reader = (RedisElementReader) reader;
		this.writer = (RedisElementWriter) writer;
	}
}
