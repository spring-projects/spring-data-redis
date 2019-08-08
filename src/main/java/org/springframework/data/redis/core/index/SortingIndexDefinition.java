package org.springframework.data.redis.core.index;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author Yan Ma
 */
public class SortingIndexDefinition extends RedisIndexDefinition implements PathBasedRedisIndexDefinition {
	public SortingIndexDefinition(String keyspace, String path) {
		super(keyspace, path, path);
		setValueTransformer(new DoubleValueTransformer());
	}

	public SortingIndexDefinition(String keyspace, String path, String indexName) {
		super(keyspace, path, indexName);
		setValueTransformer(new DoubleValueTransformer());
	}

	static class DoubleValueTransformer implements IndexValueTransformer {

		@Override
		public Double convert(Object source) {

			if (source instanceof Date) {
				Date date = (Date) source;
				return (double) date.getTime();
			} else if (source instanceof Integer) {
				Integer integer = (Integer) source;
				return (double) integer.intValue();
			} else if (source instanceof Long) {
				Long l = (Long) source;
				return (double) l.doubleValue();
			} else if (source instanceof BigDecimal) {
				BigDecimal bd = (BigDecimal) source;
				return bd.toBigInteger().doubleValue();
			} else if (source instanceof Double) {
				return (Double) source;
			} else if (source instanceof Float) {
				return (Double) source;
			} else {
				return null;
			}
		}

	}

}
