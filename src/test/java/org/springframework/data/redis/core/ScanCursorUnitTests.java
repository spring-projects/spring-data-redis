/*
 * Copyright 2014-2020 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Stack;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * @author Christoph Strobl
 */
public class ScanCursorUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	@Test // DATAREDIS-290
	public void cursorShouldNotLoopWhenNoValuesFound() {

		CapturingCursorDummy cursor = initCursor(new LinkedList<>());
		assertThat(cursor.hasNext()).isFalse();
	}

	@Test // DATAREDIS-290
	public void cursorShouldReturnNullWhenNoNextElementAvailable() {
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> initCursor(new LinkedList<>()).next());
	}

	@Test // DATAREDIS-290
	public void cursorShouldNotLoopWhenReachingStartingPointInFistLoop() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(0, "spring", "data", "redis"));
		CapturingCursorDummy cursor = initCursor(values);

		assertThat(cursor.next()).isEqualTo("spring");
		assertThat(cursor.getCursorId()).isEqualTo(0L);
		assertThat(cursor.hasNext()).isTrue();

		assertThat(cursor.next()).isEqualTo("data");
		assertThat(cursor.getCursorId()).isEqualTo(0L);
		assertThat(cursor.hasNext()).isTrue();

		assertThat(cursor.next()).isEqualTo("redis");
		assertThat(cursor.getCursorId()).isEqualTo(0L);
		assertThat(cursor.hasNext()).isFalse();
	}

	@Test // DATAREDIS-290
	public void cursorShouldStopLoopWhenReachingStartingPoint() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(0, "redis"));
		CapturingCursorDummy cursor = initCursor(values);

		assertThat(cursor.next()).isEqualTo("spring");
		assertThat(cursor.getCursorId()).isEqualTo(1L);
		assertThat(cursor.hasNext()).isTrue();

		assertThat(cursor.next()).isEqualTo("data");
		assertThat(cursor.getCursorId()).isEqualTo(2L);
		assertThat(cursor.hasNext()).isTrue();

		assertThat(cursor.next()).isEqualTo("redis");
		assertThat(cursor.getCursorId()).isEqualTo(0L);
		assertThat(cursor.hasNext()).isFalse();
	}

	@Test // DATAREDIS-290
	public void shouldThrowExceptionWhenAccessingClosedCursor() {

		CapturingCursorDummy cursor = new CapturingCursorDummy(null);

		assertThat(cursor.isClosed()).isFalse();

		exception.expect(InvalidDataAccessApiUsageException.class);
		exception.expectMessage("closed cursor");

		cursor.next();
	}

	@Test(expected = InvalidDataAccessApiUsageException.class) // DATAREDIS-290
	public void repoeningCursorShouldHappenAtLastPosition() throws IOException {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(0, "redis"));
		Cursor<String> cursor = initCursor(values).open();

		assertThat(cursor.next()).isEqualTo("spring");
		assertThat(cursor.getCursorId()).isEqualTo(1L);

		// close the cursor
		cursor.close();
		assertThat(cursor.isClosed()).isTrue();

		// reopen cursor at last position
		cursor.open();
	}

	@Test // DATAREDIS-290
	public void positionShouldBeIncrementedCorrectly() throws IOException {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(0, "redis"));
		Cursor<String> cursor = initCursor(values);

		assertThat(cursor.getPosition()).isEqualTo(0L);

		cursor.next();
		assertThat(cursor.getPosition()).isEqualTo(1L);

		cursor.next();
		assertThat(cursor.getPosition()).isEqualTo(2L);
	}

	@Test // DATAREDIS-417
	public void hasNextShouldCallScanUntilFinishedWhenScanResultIsAnEmptyCollection() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2));
		values.add(createIteration(3));
		values.add(createIteration(4));
		values.add(createIteration(5));
		values.add(createIteration(0, "redis"));
		Cursor<String> cursor = initCursor(values);

		List<String> result = new ArrayList<>();
		while (cursor.hasNext()) {
			result.add(cursor.next());
		}

		assertThat(result.size()).isEqualTo(2);
		assertThat(result).contains("spring", "redis");
	}

	@Test // DATAREDIS-417
	public void hasNextShouldStopWhenScanResultIsAnEmptyCollectionAndStateIsFinished() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2));
		values.add(createIteration(3));
		values.add(createIteration(4));
		values.add(createIteration(5));
		values.add(createIteration(6));
		values.add(createIteration(7, "data"));
		values.add(createIteration(0));
		Cursor<String> cursor = initCursor(values);

		List<String> result = new ArrayList<>();
		while (cursor.hasNext()) {
			result.add(cursor.next());
		}

		assertThat(result.size()).isEqualTo(2);
		assertThat(result).contains("spring", "data");
	}

	@Test // DATAREDIS-417
	public void hasNextShouldStopCorrectlyWhenWholeScanIterationDoesNotReturnResultsAndStateIsFinished() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1));
		values.add(createIteration(2));
		values.add(createIteration(3));
		values.add(createIteration(4));
		values.add(createIteration(5));
		values.add(createIteration(0));
		Cursor<String> cursor = initCursor(values);

		assertThat(cursor.getPosition()).isEqualTo(0L);

		int loops = 0;
		while (cursor.hasNext()) {
			cursor.next();
			loops++;
		}

		assertThat(loops).isEqualTo(0);
		assertThat(cursor.getCursorId()).isEqualTo(0L);
	}

	private CapturingCursorDummy initCursor(Queue<ScanIteration<String>> values) {
		CapturingCursorDummy cursor = new CapturingCursorDummy(values);
		cursor.open();
		return cursor;
	}

	private ScanIteration<String> createIteration(long cursorId, String... values) {
		return new ScanIteration<>(cursorId, values.length > 0 ? Arrays.asList(values) : Collections.<String> emptyList());
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
				cursors = new Stack<>();
			}
			this.cursors.push(cursorId);
			return this.values.poll();
		}
	}
}
