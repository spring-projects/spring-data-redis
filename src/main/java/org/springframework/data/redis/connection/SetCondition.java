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

	private static final SetCondition UPSERT = new SetCondition(KeyCondition.UPSERT);
	private static final SetCondition IF_ABSENT = new SetCondition(KeyCondition.IF_ABSENT);
	private static final SetCondition IF_PRESENT = new SetCondition(KeyCondition.IF_PRESENT);

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
	 * Do not set any additional command argument.
	 */
	public static SetCondition upsert() {
		return UPSERT;
	}

	/**
	 * {@code NX}
	 */
	public static SetCondition ifAbsent() {
		return IF_ABSENT;
	}

	/**
	 * {@code XX}
	 */
	public static SetCondition ifPresent() {
		return IF_PRESENT;
	}

	/**
	 * {@code IFEQ}
	 */
	public static SetCondition ifEquals(byte[] oldValue) {
		return new SetCondition(CompareCondition.ifEquals(oldValue));
	}

	/**
	 * {@code IFNE}
	 */
	public static SetCondition ifNotEquals(byte[] oldValue) {
		return new SetCondition(CompareCondition.ifNotEquals(oldValue));
	}

	/**
	 * {@code IFDEQ}
	 */
	public static SetCondition ifDigestEquals(String digest) {
		return new SetCondition(CompareCondition.ifDigestEquals(digest));
	}

	/**
	 * {@code IFDNE}
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
		 * {@code XX}
		 */
		public static KeyCondition ifPresent() {
			return IF_PRESENT;
		}

		/**
		 * {@code NX}
		 */
		public static KeyCondition ifAbsent() {
			return IF_ABSENT;
		}

	}

}
