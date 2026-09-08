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

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;

/**
 * Default {@link RedisJsonSerializer.Splitter} implementation.
 *
 * @author Mark Paluch
 */
class DefaultSplitter implements RedisJsonSerializer.Splitter {

	private final RedisJsonSerializer delegate;

	public DefaultSplitter(RedisJsonSerializer delegate) {
		this.delegate = delegate;
	}

	@Override
	public byte[] serialize(@Nullable Object value) throws SerializationException {
		return delegate.serialize(value);
	}

	@Override
	public @Nullable Object deserialize(byte @Nullable [] bytes) throws SerializationException {
		return delegate.deserialize(bytes);
	}

	@Override
	public <T> @Nullable T deserialize(byte @org.jspecify.annotations.Nullable [] source, Class<T> type)
			throws SerializationException {
		return delegate.deserialize(source, type);
	}

	@Override
	public <T> @Nullable T deserialize(byte[] source, ParameterizedTypeReference<T> typeRef)
			throws SerializationException {
		return delegate.deserialize(source, typeRef);
	}

	@Override
	public @Nullable Object deserialize(byte[] source, ResolvableType type) throws SerializationException {
		return delegate.deserialize(source, type);
	}

	@Override
	public Splitter splitter() {
		return this;
	}

}
