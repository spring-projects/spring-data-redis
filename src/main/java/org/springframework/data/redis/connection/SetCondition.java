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

import org.jspecify.annotations.Nullable;

/**
 * Condition for {@code SET} command.
 *
 * @author Yordan Tsintsov
 * @author Mark Paluch
 * @since 4.1
 */
public class SetCondition {

	private static final SetCondition UPSERT = new SetCondition(KeyCondition.upsert());
	private static final SetCondition IF_PRESENT = new SetCondition(KeyCondition.ifPresent());
	private static final SetCondition IF_ABSENT = new SetCondition(KeyCondition.ifAbsent());

	private final KeyCondition keyCondition;
	private final @Nullable CompareCondition compareCondition;

	private SetCondition(KeyCondition keyCondition) {
		this.keyCondition = keyCondition;
		this.compareCondition = null;
	}

	private SetCondition(CompareCondition compareCondition) {
		this.keyCondition = KeyCondition.UPSERT;
		this.compareCondition = compareCondition;
	}

	/**
	 * Create or update a key. Does not add {@code XX} or {@code NX} conditions to the {@code SET} command.
	 */
	public static SetCondition upsert() {
		return UPSERT;
	}

	/**
	 * Perform the {@code SET} operation only if the key is present using the {@code XX} condition.
	 */
	public static SetCondition ifPresent() {
		return IF_PRESENT;
	}

	/**
	 * Perform the {@code SET} operation only if the key is absent using the {@code NX} condition.
	 */
	public static SetCondition ifAbsent() {
		return IF_ABSENT;
	}

	/**
	 * Perform the {@code SET} operation only if the value at the key equals the {@code expectedValue} using the
	 * {@code IFEQ} condition.
	 */
	public static SetCondition ifEquals(byte[] expectedValue) {
		return new SetCondition(CompareCondition.ifEquals(expectedValue));
	}

	/**
	 * Perform the {@code SET} operation only if the value at the key does not equal the {@code expectedValue} using the
	 * {@code IFNE} condition.
	 */
	public static SetCondition ifNotEquals(byte[] expectedValue) {
		return new SetCondition(CompareCondition.ifNotEquals(expectedValue));
	}

	/**
	 * Perform the {@code SET} operation only if the value digest value equals {@code digest} using the {@code IFDEQ}
	 * condition.
	 */
	public static SetCondition ifDigestEquals(String digest) {
		return new SetCondition(CompareCondition.ifDigestEquals(digest));
	}

	/**
	 * Perform the {@code SET} operation only if the value digest value does not equal {@code digest} using the
	 * {@code IFDNE} condition.
	 */
	public static SetCondition ifDigestNotEquals(String digest) {
		return new SetCondition(CompareCondition.ifDigestNotEquals(digest));
	}

	public KeyCondition getKeyCondition() {
		return keyCondition;
	}

	public @Nullable CompareCondition getCompareCondition() {
		return compareCondition;
	}

	/**
	 * Condition for {@code SET} command key presence.
	 */
	public enum KeyCondition {

		/**
		 * Do not set any additional command argument.
		 */
		UPSERT,

		/**
		 * {@code XX}
		 */
		IF_PRESENT,

		/**
		 * {@code NX}
		 */
		IF_ABSENT;

		/**
		 * Do not set any additional command argument.
		 */
		public static KeyCondition upsert() {
			return UPSERT;
		}

		/**
		 * Perform the {@code SET} operation only if the key is present using the {@code XX} condition.
		 */
		public static KeyCondition ifPresent() {
			return IF_PRESENT;
		}

		/**
		 * Perform the {@code SET} operation only if the key is absent using the {@code NX} condition.
		 */
		public static KeyCondition ifAbsent() {
			return IF_ABSENT;
		}

	}

}
