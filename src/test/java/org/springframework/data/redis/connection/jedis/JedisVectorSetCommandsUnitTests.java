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
package org.springframework.data.redis.connection.jedis;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.connection.RedisVectorSetCommands.VAddOptions;
import org.springframework.data.redis.connection.RedisVectorSetCommands.VAddOptions.QuantizationType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.VAddParams;

/**
 * Unit tests for {@link JedisVectorSetCommands}.
 *
 * @author Anne Lee
 */
@ExtendWith(MockitoExtension.class)
class JedisVectorSetCommandsUnitTests {

	@Mock
	private JedisConnection jedisConnection;

	@Mock
	private Jedis jedis;

	@Mock
	private Pipeline pipeline;

	@Mock
	private Transaction transaction;

	@Mock
	private JedisInvoker jedisInvoker;

	@Mock
	private JedisInvoker.SingleInvocationSpec<Boolean> singleInvocationSpec;

	private JedisVectorSetCommands commands;

	private static final byte[] KEY = "test-key".getBytes();
	private static final byte[] ELEMENT = "test-element".getBytes();
	private static final byte[] FP32_VALUES = new byte[]{0, 0, -128, 63, 0, 0, 0, 64}; // Float values in FP32 format
	private static final double[] DOUBLE_VALUES = new double[]{1.5, 2.5, 3.5};

	@BeforeEach
	void setUp() {
		lenient().when(jedisConnection.invoke()).thenReturn(jedisInvoker);
		commands = new JedisVectorSetCommands(jedisConnection);
	}

	@Test
	void vAddWithFP32ValuesAndNoOptions() {
		// Given
		when(jedisInvoker.just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT)))
				.thenReturn(true);

		// When
		Boolean result = commands.vAdd(KEY, FP32_VALUES, ELEMENT, null);

		// Then
		assertThat(result).isTrue();
		verify(jedisInvoker).just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT));
	}

	@Test
	void vAddWithFP32ValuesAndOptions() {
		// Given
		VAddOptions options = VAddOptions.builder()
				.cas(true)
				.quantization(QuantizationType.Q8)
				.efBuildFactor(200)
				.maxConnections(16)
				.build();

		when(jedisInvoker.just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT), any(VAddParams.class)))
				.thenReturn(true);

		// When
		Boolean result = commands.vAdd(KEY, FP32_VALUES, ELEMENT, options);

		// Then
		assertThat(result).isTrue();
		
		ArgumentCaptor<VAddParams> paramsCaptor = ArgumentCaptor.forClass(VAddParams.class);
		verify(jedisInvoker).just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT), paramsCaptor.capture());
		
		VAddParams capturedParams = paramsCaptor.getValue();
		assertThat(capturedParams).isNotNull();
	}

	@Test
	void vAddWithFP32ValuesAndReduceDimOption() {
		// Given
		VAddOptions options = VAddOptions.builder()
				.reduceDim(128)
				.quantization(QuantizationType.NOQUANT)
				.build();

		when(jedisInvoker.just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT), eq(128), any(VAddParams.class)))
				.thenReturn(true);

		// When
		Boolean result = commands.vAdd(KEY, FP32_VALUES, ELEMENT, options);

		// Then
		assertThat(result).isTrue();
		verify(jedisInvoker).just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT), eq(128), any(VAddParams.class));
	}

	@Test
	void vAddWithDoubleValuesAndNoOptions() {
		// Given
		float[] expectedFloatValues = new float[]{1.5f, 2.5f, 3.5f};
		
		when(jedisInvoker.just(any(), any(), eq(KEY), any(float[].class), eq(ELEMENT)))
				.thenReturn(true);

		// When
		Boolean result = commands.vAdd(KEY, DOUBLE_VALUES, ELEMENT, null);

		// Then
		assertThat(result).isTrue();
		
		ArgumentCaptor<float[]> floatCaptor = ArgumentCaptor.forClass(float[].class);
		verify(jedisInvoker).just(any(), any(), eq(KEY), floatCaptor.capture(), eq(ELEMENT));
		
		float[] capturedFloats = floatCaptor.getValue();
		assertThat(capturedFloats).containsExactly(expectedFloatValues);
	}

	@Test
	void vAddWithDoubleValuesAndOptions() {
		// Given
		VAddOptions options = VAddOptions.builder()
				.cas(false)
				.quantization(QuantizationType.BIN)
				.efBuildFactor(100)
				.build();

		when(jedisInvoker.just(any(), any(), eq(KEY), any(float[].class), eq(ELEMENT), any(VAddParams.class)))
				.thenReturn(false);

		// When
		Boolean result = commands.vAdd(KEY, DOUBLE_VALUES, ELEMENT, options);

		// Then
		assertThat(result).isFalse();
		verify(jedisInvoker).just(any(), any(), eq(KEY), any(float[].class), eq(ELEMENT), any(VAddParams.class));
	}

	@Test
	void vAddWithDoubleValuesAndReduceDimOption() {
		// Given
		VAddOptions options = VAddOptions.builder()
				.reduceDim(64)
				.build();

		when(jedisInvoker.just(any(), any(), eq(KEY), any(float[].class), eq(ELEMENT), eq(64), any(VAddParams.class)))
				.thenReturn(true);

		// When
		Boolean result = commands.vAdd(KEY, DOUBLE_VALUES, ELEMENT, options);

		// Then
		assertThat(result).isTrue();
		verify(jedisInvoker).just(any(), any(), eq(KEY), any(float[].class), eq(ELEMENT), eq(64), any(VAddParams.class));
	}

	@Test
	void vAddWithAttributesInOptions() {
		// Given
		Map<String, Object> attributes = new HashMap<>();
		attributes.put("type", "fruit");
		attributes.put("color", "red");
		attributes.put("price", 2.5);

		VAddOptions options = VAddOptions.builder()
				.attributes(attributes)
				.build();

		when(jedisInvoker.just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT), any(VAddParams.class)))
				.thenReturn(true);

		// When
		Boolean result = commands.vAdd(KEY, FP32_VALUES, ELEMENT, options);

		// Then
		assertThat(result).isTrue();
		
		ArgumentCaptor<VAddParams> paramsCaptor = ArgumentCaptor.forClass(VAddParams.class);
		verify(jedisInvoker).just(any(), any(), eq(KEY), eq(FP32_VALUES), eq(ELEMENT), paramsCaptor.capture());
		
		// VAddParams should contain the JSON serialized attributes
		VAddParams capturedParams = paramsCaptor.getValue();
		assertThat(capturedParams).isNotNull();
	}

	@Test
	void vAddThrowsExceptionForInvalidAttributesSerialization() {
		// Given
		
		Map<String, Object> attributes = new HashMap<>();
		attributes.put("circular", attributes); // Circular reference causes serialization failure

		VAddOptions options = VAddOptions.builder()
				.attributes(attributes)
				.build();

		// When & Then
		assertThatThrownBy(() -> commands.vAdd(KEY, FP32_VALUES, ELEMENT, options))
				.isInstanceOf(Exception.class)
				.hasMessageContaining("Failed to serialize attributes to JSON");
	}

	@Test
	void shouldHandleNullKeyProperly() {
		// When & Then
		assertThatThrownBy(() -> commands.vAdd(null, FP32_VALUES, ELEMENT, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Key must not be null");
	}

	@Test
	void shouldHandleNullValuesProperly() {
		// When & Then - FP32 values
		assertThatThrownBy(() -> commands.vAdd(KEY, (byte[]) null, ELEMENT, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Values must not be null");

		// When & Then - Double values
		assertThatThrownBy(() -> commands.vAdd(KEY, (double[]) null, ELEMENT, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Values must not be null");
	}

	@Test
	void shouldHandleNullElementProperly() {
		// When & Then
		assertThatThrownBy(() -> commands.vAdd(KEY, FP32_VALUES, null, null))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Element must not be null");
	}

	@Test
	void shouldConvertDoubleToFloatCorrectly() {
		// Given
		double[] doubleValues = new double[]{Double.MAX_VALUE, Double.MIN_VALUE, 0.0, -1.0, 1.0};
		float[] expectedFloatValues = new float[]{Float.POSITIVE_INFINITY, 0.0f, 0.0f, -1.0f, 1.0f};

		when(jedisInvoker.just(any(), any(), eq(KEY), any(float[].class), eq(ELEMENT)))
				.thenReturn(true);

		// When
		commands.vAdd(KEY, doubleValues, ELEMENT, null);

		// Then
		ArgumentCaptor<float[]> floatCaptor = ArgumentCaptor.forClass(float[].class);
		verify(jedisInvoker).just(any(), any(), eq(KEY), floatCaptor.capture(), eq(ELEMENT));
		
		float[] capturedFloats = floatCaptor.getValue();
		assertThat(capturedFloats).containsExactly(expectedFloatValues);
	}

	@Test
	void shouldHandleAllQuantizationTypes() {
		// Test NOQUANT
		VAddOptions noquantOptions = VAddOptions.builder()
				.quantization(QuantizationType.NOQUANT)
				.build();
		
		when(jedisInvoker.just(any(), any(), any(), any(), any(), any(VAddParams.class)))
				.thenReturn(true);

		commands.vAdd(KEY, FP32_VALUES, ELEMENT, noquantOptions);

		// Test Q8
		VAddOptions q8Options = VAddOptions.builder()
				.quantization(QuantizationType.Q8)
				.build();

		commands.vAdd(KEY, FP32_VALUES, ELEMENT, q8Options);

		// Test BIN
		VAddOptions binOptions = VAddOptions.builder()
				.quantization(QuantizationType.BIN)
				.build();

		commands.vAdd(KEY, FP32_VALUES, ELEMENT, binOptions);

		// Verify all three calls were made
		verify(jedisInvoker, times(3)).just(any(), any(), any(), any(), any(), any(VAddParams.class));
	}
}
