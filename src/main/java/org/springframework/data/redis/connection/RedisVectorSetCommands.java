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
package org.springframework.data.redis.connection;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.Vector;

import java.util.Objects;

/**
 * Vector Set-specific commands supported by Redis.
 *
 * @author Anne Lee
 * @see RedisCommands
 */
@NullUnmarked
public interface RedisVectorSetCommands {

	/**
	 * Add a vector to a vector set using FP32 binary format.
	 * 
	 * @param key the key
	 * @param vector the vector as FP32 binary blob
	 * @param element the element name
	 * @param options the options for the command
	 * @return true if the element was added, false if it already existed
	 */
	Boolean vAdd(byte @NonNull [] key, byte @NonNull [] vector, byte @NonNull [] element, @Nullable VAddOptions options);

	/**
	 * Add a vector to a vector set using Vector.
	 * 
	 * @param key the key
	 * @param vector the vector
	 * @param element the element name
	 * @param options the options for the command
	 * @return true if the element was added, false if it already existed
	 */
	Boolean vAdd(byte @NonNull [] key, @NonNull Vector vector, byte @NonNull [] element, VAddOptions options);

	/**
	 * Add a vector to a vector set using double array.
	 * 
	 * @param key the key
	 * @param vector the vector as double array
	 * @param element the element name
	 * @param options the options for the command
	 * @return true if the element was added, false if it already existed
	 */
	default Boolean vAdd(byte @NonNull [] key, double @NonNull [] vector, byte @NonNull [] element, VAddOptions options) {
		return vAdd(key, Vector.unsafe(vector), element, options);
	}

	/**
	 * Options for the VADD command.
	 * 
	 * Note on attributes:
	 * - Attributes should be provided as a JSON string
	 * - The caller is responsible for JSON serialization
	 */
	class VAddOptions {

		private static final VAddOptions DEFAULT = new VAddOptions(null, false, QuantizationType.Q8, null, null, null);

		private final @Nullable Integer reduceDim;
		private final boolean cas;
		private final QuantizationType quantization;
		private final @Nullable Integer efBuildFactor;
		private final @Nullable String attributes;
		private final @Nullable Integer maxConnections;

		public VAddOptions(@Nullable Integer reduceDim, boolean cas, QuantizationType quantization,
							@Nullable Integer efBuildFactor, @Nullable String attributes,
							@Nullable Integer maxConnections) {
			this.reduceDim = reduceDim;
			this.cas = cas;
			this.quantization = quantization;
			this.efBuildFactor = efBuildFactor;
			this.attributes = attributes;
			this.maxConnections = maxConnections;
		}

		public static VAddOptions defaults() {
			return DEFAULT;
		}

		public static VAddOptions reduceDim(@Nullable Integer reduceDim) {
			return new VAddOptions(reduceDim, false, QuantizationType.Q8, null, null, null);
		}

		public static VAddOptions cas(boolean cas) {
			return new VAddOptions(null, cas, QuantizationType.Q8, null, null, null);
		}

		public static VAddOptions quantization(QuantizationType quantization) {
			return new VAddOptions(null, false, quantization, null, null, null);
		}

		public static VAddOptions efBuildFactor(@Nullable Integer efBuildFactor) {
			return new VAddOptions(null, false, QuantizationType.Q8, efBuildFactor, null, null);
		}

		public static VAddOptions attributes(@Nullable String attributes) {
			return new VAddOptions(null, false, QuantizationType.Q8, null, attributes, null);
		}

		public static VAddOptions maxConnections(@Nullable Integer maxConnections) {
			return new VAddOptions(null, false, QuantizationType.Q8, null, null, maxConnections);
		}

		public static VAddOptions casWithQuantization(boolean cas, QuantizationType quantization) {
			return new VAddOptions(null, cas, quantization, null, null, null);
		}

		public static VAddOptions reduceDimWithQuantization(Integer reduceDim, QuantizationType quantization) {
			return new VAddOptions(reduceDim, false, quantization, null, null, null);
		}

		public @Nullable Integer getReduceDim() {
			return reduceDim;
		}

		public boolean isCas() {
			return cas;
		}

		public QuantizationType getQuantization() {
			return quantization;
		}

		public @Nullable Integer getEfBuildFactor() {
			return efBuildFactor;
		}

		public @Nullable String getAttributes() {
			return attributes;
		}

		public @Nullable Integer getMaxConnections() {
			return maxConnections;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof VAddOptions that)) {return false;}
			return cas == that.cas && Objects.equals(reduceDim, that.reduceDim)
				   && quantization == that.quantization && Objects.equals(efBuildFactor, that.efBuildFactor)
				   && Objects.equals(attributes, that.attributes) && Objects.equals(maxConnections,
																					that.maxConnections);
		}

		@Override
		public int hashCode() {
			return Objects.hash(reduceDim, cas, quantization, efBuildFactor, attributes, maxConnections);
		}

		public enum QuantizationType {
			NOQUANT,
			Q8,
			BIN,
		}
	}
}
