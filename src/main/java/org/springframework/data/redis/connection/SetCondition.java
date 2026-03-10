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
import org.springframework.util.Assert;

/**
 * Condition for {@code SET} command.
 *
 * @author Yordan Tsintsov
 * @since 4.1
 */
public class SetCondition {

	private final @Nullable KeyExistence keyExistence;
	private final @Nullable CompareCondition compareCondition;

	private SetCondition(@Nullable KeyExistence keyExistence, @Nullable CompareCondition compareCondition) {

		Assert.isTrue(keyExistence != null || compareCondition != null, "Key existence or compare condition must be set");

		this.keyExistence = keyExistence;
		this.compareCondition = compareCondition;
	}

	/**
	 * Do not set any additional command argument.
	 */
	public static SetCondition upsert() {
		return new SetCondition(KeyExistence.UPSERT, null);
	}

	/**
	 * {@code NX}
	 */
	public static SetCondition ifAbsent() {
		return new SetCondition(KeyExistence.IF_ABSENT, null);
	}

	/**
	 * {@code XX}
	 */
	public static SetCondition ifPresent() {
		return new SetCondition(KeyExistence.IF_PRESENT, null);
	}

	/**
	 * {@code IFEQ}
	 */
	public static SetCondition ifEquals(byte[] oldValue) {
		CompareCondition compareCondition = CompareCondition.ifEquals(oldValue);
		return new SetCondition(null, compareCondition);
	}

	/**
	 * {@code IFNE}
	 */
	public static SetCondition ifNotEquals(byte[] oldValue) {
		CompareCondition compareCondition = CompareCondition.ifNotEquals(oldValue);
		return new SetCondition(null, compareCondition);
	}

	/**
	 * {@code IFDEQ}
	 */
	public static SetCondition ifDigestEquals(String digest) {
		CompareCondition compareCondition = CompareCondition.ifDigestEquals(digest);
		return new SetCondition(null, compareCondition);
	}

	/**
	 * {@code IFDNE}
	 */
	public static SetCondition ifDigestNotEquals(String digest) {
		CompareCondition compareCondition = CompareCondition.ifDigestNotEquals(digest);
		return new SetCondition(null, compareCondition);
	}

	public @Nullable KeyExistence getKeyExistence() {
		return keyExistence;
	}

	public @Nullable CompareCondition getCompareCondition() {
		return compareCondition;
	}

	public enum KeyExistence {

		/**
		 * Do not set any additional command argument.
		 */
		UPSERT,

		/**
		 * {@code NX}
		 */
		IF_ABSENT,

		/**
		 * {@code XX}
		 */
		IF_PRESENT

	}

}
