/*
 * Copyright 2014 the original author or authors.
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

import static org.hamcrest.core.Is.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Stack;

import org.junit.Test;

/**
 * @author Christoph Strobl
 */
public class ScanCursorUnitTests {

	/**
	 * @see DATAREDIS-290
	 */
	@Test
	public void cursorShouldNotLoopWhenNoValuesFound() {

		CapturingCursorDummy cursor = initCursor(new LinkedList<ScanIteration<String>>());
		assertThat(cursor.hasNext(), is(false));
	}

	/**
	 * @see DATAREDIS-290
	 */
	@Test(expected = NoSuchElementException.class)
	public void cursorShouldReturnNullWhenNoNextElementAvailable() {
		initCursor(new LinkedList<ScanIteration<String>>()).next();
	}

	/**
	 * @see DATAREDIS-290
	 */
	@Test
	public void cursorShouldNotLoopWhenReachingStartingPointInFistLoop() {

		LinkedList<ScanIteration<String>> values = new LinkedList<ScanIteration<String>>();
		values.add(createIteration(0, "spring", "data", "redis"));
		CapturingCursorDummy cursor = initCursor(values);

		assertThat(cursor.next(), is("spring"));
		assertThat(cursor.getCursorId(), is(0L));
		assertThat(cursor.hasNext(), is(true));

		assertThat(cursor.next(), is("data"));
		assertThat(cursor.getCursorId(), is(0L));
		assertThat(cursor.hasNext(), is(true));

		assertThat(cursor.next(), is("redis"));
		assertThat(cursor.getCursorId(), is(0L));
		assertThat(cursor.hasNext(), is(false));
	}

	/**
	 * @see DATAREDIS-290
	 */
	@Test
	public void cursorShouldStopLoopWhenReachingStartingPoint() {

		LinkedList<ScanIteration<String>> values = new LinkedList<ScanIteration<String>>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(0, "redis"));
		CapturingCursorDummy cursor = initCursor(values);

		assertThat(cursor.next(), is("spring"));
		assertThat(cursor.getCursorId(), is(1L));
		assertThat(cursor.hasNext(), is(true));

		assertThat(cursor.next(), is("data"));
		assertThat(cursor.getCursorId(), is(2L));
		assertThat(cursor.hasNext(), is(true));

		assertThat(cursor.next(), is("redis"));
		assertThat(cursor.getCursorId(), is(0L));
		assertThat(cursor.hasNext(), is(false));
	}

	private CapturingCursorDummy initCursor(Queue<ScanIteration<String>> values) {
		CapturingCursorDummy cursor = new CapturingCursorDummy(values);
		cursor.init();
		return cursor;
	}

	private ScanIteration<String> createIteration(long cursorId, String... values) {
		return new ScanIteration<String>(cursorId, Arrays.asList(values));
	}

	private class CapturingCursorDummy extends ScanCursor<String> {

		private Queue<ScanIteration<String>> values;

		private Stack<Long> cursors;

		public CapturingCursorDummy(Queue<ScanIteration<String>> values) {
			this.values = values;
		}

		@Override
		protected ScanIteration<String> doScan(long cursorId, ScanOptions options) {

			if (cursors == null) {
				cursors = new Stack<Long>();
			}
			this.cursors.push(cursorId);
			return this.values.poll();
		}
	}
}
