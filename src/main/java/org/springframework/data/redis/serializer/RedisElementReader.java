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

import java.nio.ByteBuffer;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Strategy interface that specifies a deserializer that can deserialize a binary element representation stored in Redis
 * into an object.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
@FunctionalInterface
public interface RedisElementReader<T> {

	/**
	 * Deserialize a {@link ByteBuffer} into the according type.
	 *
	 * @param buffer must not be {@literal null}.
	 * @return the deserialized value. Can be {@literal null}.
	 */
	@Nullable
	T read(ByteBuffer buffer);

	/**
	 * Create new {@link RedisElementReader} using given {@link RedisSerializer}.
	 *
	 * @param serializer must not be {@literal null}.
	 * @return new instance of {@link RedisElementReader}.
	 */
	static <T> RedisElementReader<T> from(RedisSerializer<T> serializer) {

		Assert.notNull(serializer, "Serializer must not be null!");
		return new DefaultRedisElementReader<>(serializer);
	}
}
