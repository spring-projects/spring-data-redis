package org.springframework.data.redis.core.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompositeSortingIndexDefinition serves the requirement that key name and value derive from different attributes of the
 * same parent object.
 * 
 * @author Yan Ma
 */
public class CompositeSortingIndexDefinition<T> extends SortingIndexDefinition {
	private static final Logger LOG = LoggerFactory.getLogger(CompositeSortingIndexDefinition.class);
	public IndexNameHandler<T> indexNameHandler;
	public IndexValueHandler<T> indexValueHandler;

	public CompositeSortingIndexDefinition(String keyspace, String path, IndexNameHandler<T> indexNameHandler,
			IndexValueHandler<T> indexValueHandler) {

		super(keyspace, path, path);
		this.indexNameHandler = indexNameHandler;
		this.indexValueHandler = indexValueHandler;
		setValueTransformer(new CompositeSortingIndexValueTransformer(this.indexValueHandler));
	}

	public String getIndexName(T obj) {

		T typedObj = obj;
		String indexName = null;
		try {
			indexName = indexNameHandler.getIndexName(typedObj);
		} catch (Exception e) {
			LOG.error("Thrown exception in getting index name: {}", e.getMessage());
		}
		LOG.debug("Got the index name: {}", indexName);
		return indexName;
	}

	public Object getIndexValue(T value) {

		T typedValue = (T) value;
		Double indexValue = null;
		try {
			indexValue = indexValueHandler.getValue(typedValue);
		} catch (Exception e) {
			LOG.error("Thrown exception in getting index value: {}", e.getMessage());
		}
		LOG.debug("Got the index value: {}", indexValue);
		return indexValue;
	}

	class CompositeSortingIndexValueTransformer implements IndexValueTransformer {
		IndexValueHandler<T> indexValueHandler;

		public CompositeSortingIndexValueTransformer(IndexValueHandler<T> indexValueHandler) {
			this.indexValueHandler = indexValueHandler;
		}

		@Override
		public Object convert(Object source) {

			Double value = null;
			try {
				value = indexValueHandler.getValue((T) source);
			} catch (Exception e) {
				LOG.error("Thrown exception in transforming the value: {}", e.getMessage());
			}
			LOG.debug("Got the transformed value: {}", value);
			return value;
		}

	}
}
