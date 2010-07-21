package org.springframework.datastore.keyvalue.redis;


import java.util.List;

import org.jredis.JRedis;
import org.springframework.data.core.DataMapper;
import org.springframework.data.core.QueryDefinition;
import org.springframework.datastore.core.AbstractDatastoreTemplate;

public class RedisDatastoreTemplate extends AbstractDatastoreTemplate<JRedis> {

	
	public RedisDatastoreTemplate() {
		super();
		setDatastoreConnectionFactory(new RedisConnectionFactory());
	}
	

	@Override
	public <S, T> List<T> query(QueryDefinition arg0, DataMapper<S, T> arg1) {
		return null;
	}

	
	
}
