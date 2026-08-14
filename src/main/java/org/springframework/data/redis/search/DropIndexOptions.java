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
package org.springframework.data.redis.search;

/**
 * Options for the Redis Search {@code FT.DROPINDEX} command.
 * <p>
 * Use the fluent factory method {@link #create()} to build an instance:
 * <pre class="code">
 * // Drop index only, keep documents
 * DropIndexOptions.create()
 *
 * // Drop index and delete all indexed documents
 * DropIndexOptions.create().deleteDocuments()
 * </pre>
 *
 * @author Viktoriya Kutsarova
 * @see org.springframework.data.redis.connection.RedisSearchCommands#dropIndex(String, DropIndexOptions)
 * @see <a href="https://redis.io/commands/ft.dropindex/">FT.DROPINDEX</a>
 */
public class DropIndexOptions {

	private boolean deleteDocuments;

	private DropIndexOptions() {}

	/**
	 * Create a new {@link DropIndexOptions} instance.
	 */
	public static DropIndexOptions create() {
		return new DropIndexOptions();
	}

	/**
	 * Delete all documents indexed by this index ({@code DD} flag).
	 * <p>
	 * When set, all documents that were indexed by this index will be deleted
	 * along with the index itself. Use with caution as this operation is irreversible.
	 */
	public DropIndexOptions deleteDocuments() {
		this.deleteDocuments = true;
		return this;
	}

	/**
	 * Return whether indexed documents should be deleted along with the index.
	 */
	public boolean isDeleteDocuments() {
		return deleteDocuments;
	}
}

