/*
 * Copyright 2014-2025 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;

/**
 * Unit tests for {@link ScanCursor}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class ScanCursorUnitTests {

	@Test // DATAREDIS-290
	void cursorShouldNotLoopWhenNoValuesFound() {

		CapturingCursorDummy cursor = initCursor(new LinkedList<>());
		assertThat(cursor.hasNext()).isFalse();
	}

	@Test // DATAREDIS-290
	void cursorShouldReturnNullWhenNoNextElementAvailable() {
		assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> initCursor(new LinkedList<>()).next());
	}

	@Test // DATAREDIS-290
	void cursorShouldNotLoopWhenReachingStartingPointInFistLoop() {

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
	void cursorShouldStopLoopWhenReachingStartingPoint() {

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
	void shouldThrowExceptionWhenAccessingClosedCursor() {

		CapturingCursorDummy cursor = new CapturingCursorDummy(null);

		assertThat(cursor.isClosed()).isFalse();

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(cursor::next)
				.withMessageContaining("closed cursor");
	}

	@Test // DATAREDIS-290
	void repoeningCursorShouldHappenAtLastPosition() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(0, "redis"));

		assertThatExceptionOfType(InvalidDataAccessApiUsageException.class).isThrownBy(() -> initCursor(values).open());
	}

	@Test // DATAREDIS-290
	void positionShouldBeIncrementedCorrectly() {

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
	void hasNextShouldCallScanUntilFinishedWhenScanResultIsAnEmptyCollection() {

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
	void hasNextShouldStopWhenScanResultIsAnEmptyCollectionAndStateIsFinished() {

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
	void hasNextShouldStopCorrectlyWhenWholeScanIterationDoesNotReturnResultsAndStateIsFinished() {

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

	@Test // GH-1575
	void streamLimitShouldApplyLimitation() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(3, "redis"));
		values.add(createIteration(0));

		Cursor<String> cursor = initCursor(values);

		assertThat(cursor.stream().limit(2).collect(Collectors.toList())).hasSize(2).contains("spring", "data");
	}

	@Test // GH-1575
	void streamingCursorShouldForwardClose() {

		LinkedList<ScanIteration<String>> values = new LinkedList<>();
		values.add(createIteration(1, "spring"));
		values.add(createIteration(2, "data"));
		values.add(createIteration(3, "redis"));
		values.add(createIteration(0));
		Cursor<String> cursor = initCursor(values);

		assertThat(cursor.isClosed()).isFalse();

		Stream<String> stream = cursor.stream();
		stream.collect(Collectors.toList());
		stream.close();

		assertThat(cursor.isClosed()).isTrue();
	}

	@Test // GH-2414
	void shouldCloseCursorOnScanFailure() {

		KeyBoundCursor<String> cursor = new KeyBoundCursor<String>("foo".getBytes(), Cursor.CursorId.initial(), null) {
			@Override
			protected ScanIteration<String> doScan(byte[] key, CursorId cursorId, ScanOptions options) {
				throw new IllegalStateException();
			}
		};

		assertThatIllegalStateException().isThrownBy(cursor::open);
		assertThat(cursor.isOpen()).isFalse();
		assertThat(cursor.isClosed()).isTrue();
	}

	private CapturingCursorDummy initCursor(Queue<ScanIteration<String>> values) {
		CapturingCursorDummy cursor = new CapturingCursorDummy(values);
		cursor.open();
		return cursor;
	}

	private ScanIteration<String> createIteration(long cursorId, String... values) {
		return new ScanIteration<>(Cursor.CursorId.of(cursorId),
				values.length > 0 ? Arrays.asList(values) : Collections.<String> emptyList());
	}

	private static class CapturingCursorDummy extends ScanCursor<String> {

		private final Queue<ScanIteration<String>> values;

		CapturingCursorDummy(Queue<ScanIteration<String>> values) {
			this.values = values;
		}

		@Override
		protected ScanIteration<String> doScan(CursorId cursorId, ScanOptions options) {

			ScanIteration<String> iteration = this.values.poll();

			if (iteration == null) {
				iteration = new ScanIteration<>(CursorId.initial(), Collections.emptyList());
			}
			return iteration;
		}
	}
}
