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
 * A geographic {@link SchemaField} for radius-based queries on longitude/latitude pairs
 * stored as comma-separated strings.
 * <p>
 * Create instances via {@link SchemaField#geo(String)}.
 *
 * @author Viktoriya Kutsarova
 * @see SchemaField#geo(String)
 * @see <a href="https://redis.io/commands/ft.create/">FT.CREATE — GEO field options</a>
 */
public final class GeoField extends SortableSchemaField<GeoField> {

	GeoField(String name) {
		super(name);
	}
}
