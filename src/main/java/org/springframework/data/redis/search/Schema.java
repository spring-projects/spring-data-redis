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

import org.springframework.data.redis.connection.RedisSearchCommands;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Describes the fields of a Redis Search index. A {@code Schema} is passed to
 * {@link RedisSearchCommands#createIndex createIndex}, optionally with an {@link IndexDefinition}.
 *
 * <pre class="code">
 * Schema schema = Schema.of(
 *     SchemaField.text("title").weight(2.0).sortable(),
 *     SchemaField.tag("status"),
 *     SchemaField.numeric("price").sortable()
 * );
 * </pre>
 *
 * @author Viktoriya Kutsarova
 * @see SchemaField
 * @see IndexDefinition
 */
public class Schema {

	private final List<SchemaField> fields;

	private Schema(List<SchemaField> fields) {
		this.fields = Collections.unmodifiableList(fields);
	}

	/**
	 * Create a {@link Schema} from the given field definitions.
	 *
	 * @param fields one or more field definitions
	 */
	public static Schema of(SchemaField... fields) {
		return new Schema(Arrays.asList(fields));
	}

	/**
	 * Create a {@link Schema} from the given list of field definitions.
	 *
	 * @param fields list of field definitions
	 */
	public static Schema fromList(List<SchemaField> fields) {
		return new Schema(fields);
	}

	/**
	 * Return the ordered list of field definitions in this schema.
	 */
	public List<SchemaField> getFields() {
		return fields;
	}
}
