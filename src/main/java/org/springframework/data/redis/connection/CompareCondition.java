/*
 * Copyright 2026-present the original author or authors.
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

import org.springframework.util.Assert;

/**
 * Value object representing a condition for conditional operations such as {@code DELEX} and {@code SET}. The condition
 * supports by-value and by-digest comparison types with equality and inequality operators.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.1
 */
public class CompareCondition {

	private final boolean equal;

	private final Value value;

	private final Comparison comparison;

	private CompareCondition(boolean equal, byte[] value, Comparison comparison) {
		this(equal, new BytesValue(value), comparison);
	}

	private CompareCondition(boolean equal, Value value, Comparison comparison) {
		this.equal = equal;
		this.value = value;
		this.comparison = comparison;
	}

	/**
	 * Create a new {@link CompareCondition} for value comparison matching if the key value equals the given
	 * {@code value}.
	 *
	 * @param value the comparison value.
	 * @return the new {@link CompareCondition}.
	 */
	public static CompareCondition ifEquals(byte[] value) {
		return new CompareCondition(true, value, Comparison.VALUE);
	}

	/**
	 * Create a new {@link CompareCondition} for value comparison matching if the key value does not equal the given
	 * {@code value}.
	 *
	 * @param value the comparison value.
	 * @return the new {@link CompareCondition}.
	 */
	public static CompareCondition ifNotEquals(byte[] value) {
		return new CompareCondition(false, value, Comparison.VALUE);
	}

	/**
	 * Create a new {@link CompareCondition} for {@code digest} comparison matching if the key value equals the given
	 * {@code hex16Digest}.
	 *
	 * @param hex16Digest the value digest, see Redis {@code DIGEST} command.
	 * @return the new {@link CompareCondition}.
	 */
	public static CompareCondition ifDigestEquals(String hex16Digest) {
		return new CompareCondition(true, new DigestValue(hex16Digest), Comparison.DIGEST);
	}

	/**
	 * Create a new {@link CompareCondition} for {@code digest} comparison matching if the key value does not equal the
	 * given {@code hex16Digest}.
	 *
	 * @param hex16Digest the value digest, see Redis {@code DIGEST} command.
	 * @return the new {@link CompareCondition}.
	 */
	public static CompareCondition ifDigestNotEquals(String hex16Digest) {
		return new CompareCondition(false, new DigestValue(hex16Digest), Comparison.DIGEST);
	}

	public boolean isEqual() {
		return equal;
	}

	public Value getValue() {
		return value;
	}

	public Comparison getComparison() {
		return comparison;
	}

	/**
	 * Comparison type for {@code DELEX} command.
	 */
	public enum Comparison {

		VALUE, DIGEST;

		/**
		 * By-value comparison {@code IFEQ}, {@code IFNE}.
		 */
		public static Comparison value() {
			return VALUE;
		}

		/**
		 * By-digest comparison {@code IFDEQ}, {@code IFDNE}.
		 */
		public static Comparison getDigest() {
			return DIGEST;
		}

	}

	/**
	 * Value abstraction to capture a value or digest preserving its internal representation. A value can be represented
	 * as a byte array or a {@link #toString()}.
	 */
	public interface Value {

		/**
		 * @return the value as byte array.
		 */
		byte[] asBytes();

	}

	private record BytesValue(byte[] value) implements Value {

		BytesValue {
			Assert.notNull(value, "Value must not be null");
		}

		@Override
		public byte[] asBytes() {
			return value;
		}

		@Override
		public String toString() {
			return new String(asBytes());
		}

	}

	private record DigestValue(String digest) implements Value {

		DigestValue {
			Assert.notNull(digest, "Digest must not be null");
		}

		@Override
		public byte[] asBytes() {
			return digest.getBytes();
		}

		@Override
		public String toString() {
			return digest;
		}

	}

}
