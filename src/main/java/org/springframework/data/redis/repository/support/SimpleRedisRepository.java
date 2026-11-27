package org.springframework.data.redis.repository.support;

import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.redis.repository.RedisRepository;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.core.EntityInformation;

/**
 * Redis specific repository implementation.
 *
 * @author Junghoon Ban
 * @param <T>
 * @param <ID>
 */
@NoRepositoryBean
public class SimpleRedisRepository<T, ID> extends SimpleKeyValueRepository<T, ID> implements RedisRepository<T, ID> {

	public SimpleRedisRepository(EntityInformation<T, ID> metadata, KeyValueOperations operations) {
		super(metadata, operations);
	}
}
