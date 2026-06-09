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

/**
 * Condition for {@code JSON.SET} command.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
public class JsonSetCondition {

	private static final JsonSetCondition UPSERT = new JsonSetCondition(PathCondition.UPSERT);
	private static final JsonSetCondition IF_PATH_NOT_EXISTS = new JsonSetCondition(PathCondition.IF_PATH_NOT_EXISTS);
	private static final JsonSetCondition IF_PATH_EXISTS = new JsonSetCondition(PathCondition.IF_PATH_EXISTS);

	private final PathCondition pathCondition;

	private JsonSetCondition(PathCondition pathCondition) {
		this.pathCondition = pathCondition;
	}

	/**
	 * Do not set any additional command argument.
	 *
	 * @return {@link JsonSetCondition#UPSERT}
	 */
	public static JsonSetCondition upsert() {
		return UPSERT;
	}

	/**
	 * Perform the {@code JSON.SET} operation only if the path does not exist.
	 *
	 * @return {@link JsonSetCondition#IF_PATH_NOT_EXISTS}
	 */
	public static JsonSetCondition ifPathNotExists() {
		return IF_PATH_NOT_EXISTS;
	}

	/**
	 * Perform the {@code JSON.SET} operation only if the path exists.
	 *
	 * @return {@link JsonSetCondition#IF_PATH_EXISTS}
	 */
	public static JsonSetCondition ifPathExists() {
		return IF_PATH_EXISTS;
	}

	/**
	 * Get the path condition.
	 *
	 * @return the path condition.
	 */
	public PathCondition getPathCondition() {
		return pathCondition;
	}

	/**
	 * Condition for {@code JSON.SET} command path presence.
	 */
	public enum PathCondition {

		/**
		 * Do not set any additional command argument.
		 */
		UPSERT,

		/**
		 * {@code NX}
		 */
		IF_PATH_NOT_EXISTS,

		/**
		 * {@code XX}
		 */
		IF_PATH_EXISTS

	}

}
