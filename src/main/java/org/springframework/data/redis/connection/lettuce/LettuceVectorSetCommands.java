/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;


import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.domain.Vector;
import org.springframework.data.redis.connection.RedisVectorSetCommands;
import org.springframework.util.Assert;

/**
 {@link RedisVectorSetCommands} implementation for {@literal Lettuce}.
 *
 * @author Anne Lee
 * @since 3.5
 */
@NullUnmarked
class LettuceVectorSetCommands implements RedisVectorSetCommands {

	private final LettuceConnection connection;

	LettuceVectorSetCommands(LettuceConnection connection) {
		this.connection = connection;
	}

	@Override
	public Boolean vAdd(byte[] key, byte[] vector, byte[] element, VAddOptions options) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(vector, "Vector must not be null");
		Assert.notNull(element, "Element must not be null");

		// TODO: Implement when Lettuce adds native support for V.ADD
		// For now, we need to use custom command execution
		throw new UnsupportedOperationException("V.ADD is not yet supported in Lettuce");
	}

	@Override
	public Boolean vAdd(byte @NonNull [] key, @NonNull Vector vector, byte @NonNull [] element,
						VAddOptions options) {
		// TODO: Implement when Lettuce adds native support for V.ADD
		// For now, we need to use custom command execution
		throw new UnsupportedOperationException("V.ADD is not yet supported in Lettuce");
	}
}
