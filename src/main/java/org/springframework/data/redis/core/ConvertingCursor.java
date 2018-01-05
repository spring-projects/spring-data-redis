/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis.core;

import java.io.IOException;

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

	private Cursor<S> delegate;
	private Converter<S, T> converter;

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

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return delegate.hasNext();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	@Nullable
	public T next() {
		return converter.convert(delegate.next());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		delegate.remove();
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		delegate.close();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getCursorId()
	 */
	@Override
	public long getCursorId() {
		return delegate.getCursorId();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return delegate.isClosed();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#open()
	 */
	@Override
	public Cursor<T> open() {
		this.delegate = delegate.open();
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getPosition()
	 */
	@Override
	public long getPosition() {
		return delegate.getPosition();
	}

}
