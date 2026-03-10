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
 * Comparison-based condition for conditional Redis operations such as {@code DELEX} and {@code SET}.
 * {@code CompareCondition} objects specify how to compare the existing value with an expected value, supporting both
 * direct value comparison and digest-based comparison for efficient verification of large values.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.1
 */
public class CompareCondition {

	private final ComparisonFunction function;

	private final ComparisonOperator operator;

	private final Value value;

	private CompareCondition(ComparisonFunction function, ComparisonOperator operator, byte[] value) {
		this(function, operator, new BytesValue(value));
	}

	private CompareCondition(ComparisonFunction function, ComparisonOperator operator, Value value) {
		this.function = function;
		this.operator = operator;
		this.value = value;
	}

	/**
	 * Create a new {@link CompareCondition} for value comparison matching if the key value equals the given
	 * {@code value}.
	 *
	 * @param value the comparison value.
	 * @return the new {@link CompareCondition}.
	 */
	public static CompareCondition ifEquals(byte[] value) {
		return new CompareCondition(ComparisonFunction.value(), ComparisonOperator.equals(), value);
	}

	/**
	 * Create a new {@link CompareCondition} for value comparison matching if the key value does not equal the given
	 * {@code value}.
	 *
	 * @param value the comparison value.
	 * @return the new {@link CompareCondition}.
	 */
	public static CompareCondition ifNotEquals(byte[] value) {
		return new CompareCondition(ComparisonFunction.value(), ComparisonOperator.notEquals(), value);
	}

	/**
	 * Create a new {@link CompareCondition} for {@code digest} comparison matching if the key value equals the given
	 * {@code hex16Digest}.
	 *
	 * @param hex16Digest the value digest in hexadecimal format. Obtained via Redis {@code DIGEST} command.
	 * @return the new {@link CompareCondition}.
	 * @see <a href="https://redis.io/commands/digest">Redis Documentation: DIGEST</a>
	 */
	public static CompareCondition ifDigestEquals(String hex16Digest) {
		return new CompareCondition(ComparisonFunction.digest(), ComparisonOperator.equals(), new DigestValue(hex16Digest));
	}

	/**
	 * Create a new {@link CompareCondition} for {@code digest} comparison matching if the key value does not equal the
	 * given {@code hex16Digest}.
	 *
	 * @param hex16Digest the value digest in hexadecimal format. Obtained via Redis {@code DIGEST} command.
	 * @return the new {@link CompareCondition}.
	 * @see <a href="https://redis.io/commands/digest">Redis Documentation: DIGEST</a>
	 */
	public static CompareCondition ifDigestNotEquals(String hex16Digest) {
		return new CompareCondition(ComparisonFunction.digest(), ComparisonOperator.notEquals(),
				new DigestValue(hex16Digest));
	}

	public ComparisonFunction getComparison() {
		return function;
	}

	public ComparisonOperator getOperator() {
		return operator;
	}

	public Value getValue() {
		return value;
	}

	/**
	 * Predicate types.
	 */
	public enum ComparisonOperator {

		EQUALS, NOT_EQUALS;

		/**
		 * Equals comparison operator.
		 */
		public static ComparisonOperator equals() {
			return EQUALS;
		}

		/**
		 * Not-equals (i.e. inequality) comparison operator.
		 */
		public static ComparisonOperator notEquals() {
			return NOT_EQUALS;
		}

	}

	/**
	 * Comparison type for {@code DELEX} and {@code SET} command.
	 */
	public enum ComparisonFunction {

		VALUE, DIGEST;

		/**
		 * By-value comparison {@code IFEQ}, {@code IFNE}.
		 */
		public static ComparisonFunction value() {
			return VALUE;
		}

		/**
		 * By-digest comparison {@code IFDEQ}, {@code IFDNE}.
		 */
		public static ComparisonFunction digest() {
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
