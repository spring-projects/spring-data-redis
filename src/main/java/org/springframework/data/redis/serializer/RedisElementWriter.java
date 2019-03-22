/*
 * Copyright 2017 the original author or authors.
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

import java.nio.ByteBuffer;

/**
 * Strategy interface that specifies a serializer that can serialize an element to its binary representation to be used
 * as Redis protocol payload.
 *
 * @author Mark Paluch
 */
@FunctionalInterface
public interface RedisElementWriter<T> {

	/**
	 * Serialize a {@code element} to its {@link ByteBuffer} representation.
	 *
	 * @param element
	 * @return the {@link ByteBuffer} representing {@code element} in its binary form.
	 */
	ByteBuffer write(T element);
}
