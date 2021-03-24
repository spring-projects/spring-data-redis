/*
 * Copyright 2014-2021 the original author or authors.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Redis client agnostic {@link Cursor} implementation continuously loading additional results from Redis server until
 * reaching its starting point {@code zero}. <br />
 * <strong>Note:</strong> Please note that the {@link ScanCursor} has to be initialized ({@link #open()} prior to usage.
 *
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Duobiao Ou
 * @author Marl Paluch
 * @param <T>
 * @since 1.4
 */
public abstract class ScanCursor<T> implements Cursor<T> {

	private CursorState state;
	private long cursorId;
	private Iterator<T> delegate;
	private final ScanOptions scanOptions;
	private long position;

	/**
	 * Crates new {@link ScanCursor} with {@code id=0} and {@link ScanOptions#NONE}
	 */
	public ScanCursor() {
		this(ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor} with {@code id=0}.
	 *
	 * @param options the scan options to apply.
	 */
	public ScanCursor(ScanOptions options) {
		this(0, options);
	}

	/**
	 * Crates new {@link ScanCursor} with {@link ScanOptions#NONE}
	 *
	 * @param cursorId the cursor Id.
	 */
	public ScanCursor(long cursorId) {
		this(cursorId, ScanOptions.NONE);
	}

	/**
	 * Crates new {@link ScanCursor}
	 *
	 * @param cursorId the cursor Id.
	 * @param options Defaulted to {@link ScanOptions#NONE} if {@code null}.
	 */
	public ScanCursor(long cursorId, @Nullable ScanOptions options) {

		this.scanOptions = options != null ? options : ScanOptions.NONE;
		this.cursorId = cursorId;
		this.state = CursorState.READY;
		this.delegate = Collections.emptyIterator();
	}

	/**
	 * Crates a new {@link ScanCursor}.
	 *
	 * @param source source cursor.
	 * @since 2.5
	 */
	private ScanCursor(ScanCursor<T> source) {

		this.scanOptions = source.scanOptions;
		this.cursorId = source.cursorId;
		this.state = source.state;
		this.delegate = source.delegate;
	}

	private void scan(long cursorId) {

		ScanIteration<T> result = doScan(cursorId, this.scanOptions);
		processScanResult(result);
	}

	/**
	 * Performs the actual scan command using the native client implementation. The given {@literal options} are never
	 * {@code null}.
	 *
	 * @param cursorId
	 * @param options
	 * @return
	 */
	protected abstract ScanIteration<T> doScan(long cursorId, ScanOptions options);

	/**
	 * Initialize the {@link Cursor} prior to usage.
	 */
	public final ScanCursor<T> open() {

		if (!isReady()) {
			throw new InvalidDataAccessApiUsageException("Cursor already " + state + ". Cannot (re)open it.");
		}

		state = CursorState.OPEN;
		doOpen(cursorId);

		return this;
	}

	/**
	 * Customization hook when calling {@link #open()}.
	 *
	 * @param cursorId
	 */
	protected void doOpen(long cursorId) {
		scan(cursorId);
	}

	private void processScanResult(ScanIteration<T> result) {

		cursorId = result.getCursorId();

		if (isFinished(cursorId)) {
			state = CursorState.FINISHED;
		}

		if (!CollectionUtils.isEmpty(result.getItems())) {
			delegate = result.iterator();
		} else {
			resetDelegate();
		}
	}

	/**
	 * Check whether {@code cursorId} is finished.
	 *
	 * @param cursorId the cursor Id
	 * @return {@literal true} if the cursor is considered finished, {@literal false} otherwise.s
	 * @since 2.1
	 */
	protected boolean isFinished(long cursorId) {
		return cursorId == 0;
	}

	private void resetDelegate() {
		delegate = Collections.emptyIterator();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getCursorId()
	 */
	@Override
	public long getCursorId() {
		return cursorId;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {

		assertCursorIsOpen();

		if (limitReached(getPosition())) {
			return false;
		}

		while (!delegate.hasNext() && !CursorState.FINISHED.equals(state)) {
			scan(cursorId);
		}

		if (delegate.hasNext()) {
			return true;
		}

		return cursorId > 0;
	}

	/**
	 * Evaluate if the current cursor position has reached a point where it should stop.
	 *
	 * @param currentPosition the current position.
	 * @return {@literal false} by default.
	 * @since 2.5
	 */
	protected boolean limitReached(long currentPosition) {
		return false;
	}

	private void assertCursorIsOpen() {

		if (isReady() || isClosed()) {
			throw new InvalidDataAccessApiUsageException("Cannot access closed cursor. Did you forget to call open()?");
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {

		assertCursorIsOpen();

		if (!hasNext()) {
			throw new NoSuchElementException("No more elements available for cursor " + cursorId + ".");
		}

		T next = moveNext(delegate);
		position++;

		return next;
	}

	/**
	 * Fetch the next item from the underlying {@link Iterable}.
	 *
	 * @param source
	 * @return
	 */
	protected T moveNext(Iterator<T> source) {
		return source.next();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException("Remove is not supported");
	}

	/*
	 * (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public final void close() {

		try {
			doClose();
		} finally {
			state = CursorState.CLOSED;
		}
	}

	/**
	 * Customization hook for cleaning up resources on when calling {@link #close()}.
	 */
	protected void doClose() {}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#isClosed()
	 */
	@Override
	public boolean isClosed() {
		return state == CursorState.CLOSED;
	}

	protected final boolean isReady() {
		return state == CursorState.READY;
	}

	protected final boolean isOpen() {
		return state == CursorState.OPEN;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#getPosition()
	 */
	@Override
	public long getPosition() {
		return position;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.Cursor#limit(long)
	 */
	@Override
	public ScanCursor<T> limit(long count) {

		Assert.isTrue(count >= 0, "Count must be greater or equal to zero");

		return new LimitingCursor<>(this, count);
	}

	/**
	 * @author Thomas Darimont
	 */
	enum CursorState {
		READY, OPEN, FINISHED, CLOSED;
	}

	/**
	 * Wrapper for a concrete {@link ScanCursor} forwarding {@link #doScan(long, ScanOptions)}, {@link #doClose()} and
	 * {@link #isClosed()}.
	 *
	 * @param <T>
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 * @since 2.5
	 */
	private static class LimitingCursor<T> extends ScanCursor<T> {

		private final ScanCursor<T> delegate;
		private final long limit;

		LimitingCursor(ScanCursor<T> delegate, long limit) {

			super(delegate);

			this.delegate = delegate;
			this.limit = limit;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.ScanCursor#doScan(long, ScanOptions)
		 */
		@Override
		protected ScanIteration<T> doScan(long cursorId, ScanOptions options) {
			return delegate.doScan(cursorId, options);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.ScanCursor#doClose()
		 */
		@Override
		protected void doClose() {
			delegate.close();
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
		 * @see org.springframework.data.redis.core.ScanCursor#limitReached(long)
		 */
		@Override
		protected boolean limitReached(long currentPosition) {
			return delegate.limitReached(currentPosition) || (limit != -1 && currentPosition > limit - 1);
		}
	}
}
