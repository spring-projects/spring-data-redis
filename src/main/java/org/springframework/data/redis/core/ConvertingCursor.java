/*
 * Copyright 2014-2022 the original author or authors.
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
package org.springframework.data.redis.core;

import org.springframework.core.convert.converter.Converter;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ConvertingCursor} wraps a given cursor and applies given {@link Converter} to items prior to returning them.
 * This allows to easily perform required conversion whereas the underlying implementation may still work with its
 * native types.
 *
 * @author Christoph Strobl
 * @param <S>
 * @param <T>
 * @since 1.4
 */
public class ConvertingCursor<S, T> implements Cursor<T> {

	private final Cursor<S> delegate;
	private final Converter<S, T> converter;

	/**
	 * @param cursor Cursor must not be {@literal null}.
	 * @param converter Converter must not be {@literal null}.
	 */
	public ConvertingCursor(Cursor<S> cursor, Converter<S, T> converter) {

		Assert.notNull(cursor, "Cursor delegate must not be 'null'.");
		Assert.notNull(cursor, "Converter must not be 'null'.");
		this.delegate = cursor;
		this.converter = converter;
	}

	@Override
	public boolean hasNext() {
		return delegate.hasNext();
	}

	@Override
	@Nullable
	public T next() {
		return converter.convert(delegate.next());
	}

	@Override
	public void remove() {
		delegate.remove();
	}

	@Override
	public void close() {
		delegate.close();
	}

	@Override
	public long getCursorId() {
		return delegate.getCursorId();
	}

	@Override
	public boolean isClosed() {
		return delegate.isClosed();
	}

	@Override
	public long getPosition() {
		return delegate.getPosition();
	}
}
