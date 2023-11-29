/*
 * Copyright 2016-2023 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.index.CompositeSortingIndexDefinition;
import org.springframework.data.redis.core.index.GeoIndexDefinition;
import org.springframework.data.redis.core.index.IndexDefinition;
import org.springframework.data.redis.core.index.SimpleIndexDefinition;
import org.springframework.data.redis.core.index.SortingIndexDefinition;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 1.8
 */
class IndexedDataFactoryProvider {

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	@Nullable
	IndexedDataFactory getIndexedDataFactory(IndexDefinition definition) {

		if (definition instanceof SimpleIndexDefinition) {
			return new SimpleIndexedPropertyValueFactory((SimpleIndexDefinition) definition);
		} else if (definition instanceof GeoIndexDefinition) {
			return new GeoIndexedPropertyValueFactory(((GeoIndexDefinition) definition));
		} else if (definition instanceof SortingIndexDefinition){
			return new SortingIndexedPropertyValueFactory(((SortingIndexDefinition) definition));
		} 
		return null;
	}

	static interface IndexedDataFactory {
		IndexedData createIndexedDataFor(Object value);
	}
	
	static class SortingIndexedPropertyValueFactory implements IndexedDataFactory {
		final SortingIndexDefinition indexDefinition;

		public SortingIndexedPropertyValueFactory(SortingIndexDefinition indexDefinition) {
			this.indexDefinition = indexDefinition;
		}

		@Override
		public IndexedData createIndexedDataFor(Object value) {
			if (indexDefinition instanceof CompositeSortingIndexDefinition) {
				CompositeSortingIndexDefinition csid = (CompositeSortingIndexDefinition) indexDefinition;
				return new SortingIndexedPropertyValue(indexDefinition.getKeyspace(), csid.getIndexName(value),
						csid.getIndexValue(value));
			} else {
				return new SortingIndexedPropertyValue(indexDefinition.getKeyspace(), indexDefinition.getIndexName(),
						indexDefinition.valueTransformer().convert(value));
			}
		}

	}
	
	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	static class SimpleIndexedPropertyValueFactory implements IndexedDataFactory {

		final SimpleIndexDefinition indexDefinition;

		SimpleIndexedPropertyValueFactory(SimpleIndexDefinition indexDefinition) {
			this.indexDefinition = indexDefinition;
		}

		public SimpleIndexedPropertyValue createIndexedDataFor(Object value) {

			return new SimpleIndexedPropertyValue(indexDefinition.getKeyspace(), indexDefinition.getIndexName(),
					indexDefinition.valueTransformer().convert(value));
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.8
	 */
	static class GeoIndexedPropertyValueFactory implements IndexedDataFactory {

		final GeoIndexDefinition indexDefinition;

		public GeoIndexedPropertyValueFactory(GeoIndexDefinition indexDefinition) {
			this.indexDefinition = indexDefinition;
		}

		public GeoIndexedPropertyValue createIndexedDataFor(Object value) {

			return new GeoIndexedPropertyValue(indexDefinition.getKeyspace(), indexDefinition.getPath(),
					(Point) indexDefinition.valueTransformer().convert(value));
		}
	}
}
