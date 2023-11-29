package org.springframework.data.redis.core.index;
/**
 * @author Yan Ma
 */
public interface IndexValueHandler<Input> {
	public Double getValue(Input input);
}
