package org.springframework.data.redis.core.index;
/**
 * @author Yan Ma
 */
public interface IndexNameHandler<T> {
	String getIndexName(T t);
}
