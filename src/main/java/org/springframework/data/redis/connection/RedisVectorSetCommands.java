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

import java.util.HashMap;
import java.util.Map;

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
	 * @param values the vector as FP32 binary blob
	 * @param element the element name
	 * @param options the options for the command
	 * @return true if the element was added, false if it already existed
	 */
	Boolean vAdd(byte @NonNull [] key, byte @NonNull [] values, byte @NonNull [] element, VAddOptions options);

	/**
	 * Add a vector to a vector set using double array.
	 * 
	 * @param key the key
	 * @param values the vector as double array
	 * @param element the element name
	 * @param options the options for the command
	 * @return true if the element was added, false if it already existed
	 */
	Boolean vAdd(byte @NonNull [] key, double @NonNull [] values, byte @NonNull [] element, VAddOptions options);

	/**
	 * Options for the VADD command.
	 * 
	 * Note on attributes:
	 * - Attributes are serialized to JSON and must be JavaScript/JSON compatible types
	 * - Supported types: String, Number (Integer, Long, Double, Float), Boolean, null
	 * - Collections (List, Map) are supported for nested structures
	 * - Custom objects require proper JSON serialization support
	 * - Date/Time objects should be converted to String or timestamp before use
	 */
	class VAddOptions {
		private final @Nullable Integer reduceDim;
		private final boolean cas;
		private final QuantizationType quantization;
		private final @Nullable Integer efBuildFactor;
		private final @Nullable Map<String, Object> attributes;
		private final @Nullable Integer maxConnections;

		private VAddOptions(Builder builder) {
			this.reduceDim = builder.reduceDim;
			this.cas = builder.cas;
			this.quantization = builder.quantization;
			this.efBuildFactor = builder.efBuildFactor;
			this.attributes = builder.attributes;
			this.maxConnections = builder.maxConnections;
		}

		public static Builder builder() {
			return new Builder();
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

		public @Nullable Map<String, Object> getAttributes() {
			return attributes;
		}

		public @Nullable Integer getMaxConnections() {
			return maxConnections;
		}

		public static class Builder {
			private @Nullable Integer reduceDim;
			private boolean cas = false;
			private QuantizationType quantization = QuantizationType.Q8;
			private @Nullable Integer efBuildFactor;
			private @Nullable Map<String, Object> attributes;
			private @Nullable Integer maxConnections;

			private Builder() {}

			public Builder reduceDim(@Nullable Integer reduceDim) {
				this.reduceDim = reduceDim;
				return this;
			}

			public Builder cas(boolean cas) {
				this.cas = cas;
				return this;
			}

			public Builder quantization(QuantizationType quantization) {
				this.quantization = quantization;
				return this;
			}

			public Builder efBuildFactor(@Nullable Integer efBuildFactor) {
				this.efBuildFactor = efBuildFactor;
				return this;
			}

			public Builder attributes(@Nullable Map<String, Object> attributes) {
				this.attributes = attributes;
				return this;
			}

			public Builder attribute(String key, Object value) {
				if (this.attributes == null) {
					this.attributes = new HashMap<>();
				}
				this.attributes.put(key, value);
				return this;
			}

			public Builder maxConnections(@Nullable Integer maxConnections) {
				this.maxConnections = maxConnections;
				return this;
			}

			public VAddOptions build() {
				return new VAddOptions(this);
			}
		}

		public enum QuantizationType {
			NOQUANT,
			Q8,
			BIN,
		}
	}
}
