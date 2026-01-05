/*
 * Copyright 2017-present the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.Test;

import org.springframework.core.convert.converter.Converter;

/**
 * Unit Tests for {@link ConvertingCursor}.
 *
 * @author John Blum
 */
@SuppressWarnings("unchecked")
class ConvertingCursorUnitTests {

	@Test // #2701
	void constructConvertingCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		new ConvertingCursor<>(mockCursor, mockConverter);

		verifyNoInteractions(mockConverter, mockCursor);
	}

	@Test // #2701
	@SuppressWarnings("all")
	void constructConvertingCursorWithNullConverter() {

		Cursor<Object> mockCursor = mock(Cursor.class);

		assertThatIllegalArgumentException().isThrownBy(() -> new ConvertingCursor<>(mockCursor, null))
				.withMessage("Converter must not be null").withNoCause();

		verifyNoInteractions(mockCursor);
	}

	@Test // #2701
	@SuppressWarnings("all")
	void constructConvertingCursorWithNullCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);

		assertThatIllegalArgumentException().isThrownBy(() -> new ConvertingCursor<>(null, mockConverter))
				.withMessage("Cursor must not be null").withNoCause();

		verifyNoInteractions(mockConverter);
	}

	@Test
	void hasNextDelegatesToCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		doReturn(true).when(mockCursor).hasNext();

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		assertThat(convertingCursor.hasNext()).isTrue();

		verify(mockCursor, times(1)).hasNext();
		verifyNoMoreInteractions(mockCursor);
		verifyNoInteractions(mockConverter);
	}

	@Test
	void nextDelegatesToCursorAndIsConverted() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		doReturn("test").when(mockCursor).next();
		doAnswer(invocation -> invocation.getArgument(0).toString().toUpperCase()).when(mockConverter).convert(any());

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		assertThat(convertingCursor.next()).isEqualTo("TEST");

		verify(mockCursor, times(1)).next();
		verify(mockConverter, times(1)).convert(eq("test"));
		verifyNoMoreInteractions(mockCursor, mockConverter);
	}

	@Test
	void removeDelegatesToCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		convertingCursor.remove();

		verify(mockCursor, times(1)).remove();
		verifyNoMoreInteractions(mockCursor);
		verifyNoInteractions(mockConverter);
	}

	@Test
	void closeDelegatesToCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		convertingCursor.close();

		verify(mockCursor, times(1)).close();
		verifyNoMoreInteractions(mockCursor);
		verifyNoInteractions(mockConverter);
	}

	@Test
	void getCursorIdDelegatesToCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		doReturn(1L).when(mockCursor).getCursorId();

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		assertThat(convertingCursor.getCursorId()).isOne();

		verify(mockCursor, times(1)).getCursorId();
		verifyNoMoreInteractions(mockCursor);
		verifyNoInteractions(mockConverter);
	}

	@Test
	void isClosedDelegatesToCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		doReturn(false).when(mockCursor).isClosed();

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		assertThat(convertingCursor.isClosed()).isFalse();

		verify(mockCursor, times(1)).isClosed();
		verifyNoMoreInteractions(mockCursor);
		verifyNoInteractions(mockConverter);
	}

	@Test
	void getPositionDelegatesToCursor() {

		Converter<Object, Object> mockConverter = mock(Converter.class);
		Cursor<Object> mockCursor = mock(Cursor.class);

		doReturn(12L).when(mockCursor).getPosition();

		ConvertingCursor<?, ?> convertingCursor = new ConvertingCursor<>(mockCursor, mockConverter);

		assertThat(convertingCursor.getPosition()).isEqualTo(12L);

		verify(mockCursor, times(1)).getPosition();
		verifyNoMoreInteractions(mockCursor);
		verifyNoInteractions(mockConverter);
	}
}
