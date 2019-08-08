package org.springframework.data.redis.core.index;

import static org.junit.Assert.*;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.repository.configuration.EnableRedisRepositories;
import org.springframework.data.redis.test.util.RelaxedJUnit4ClassRunner;
import org.springframework.test.context.ContextConfiguration;

import redis.clients.jedis.JedisPoolConfig;

/**
 * @author Yan Ma
 */
@RunWith(RelaxedJUnit4ClassRunner.class)
@ContextConfiguration
public class RedisRepositoryIndexTest {
    
    final Log logger = LogFactory.getLog(getClass());
    
  	@Configuration
  	@EnableRedisRepositories(indexConfiguration=TestIndexConfiguration.class)
  	public static class Config {

			@Bean
      RedisConnectionFactory connectionFactory() {
				
  			JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
  			connectionFactory.setHostName(SettingsUtils.getHost());
  			connectionFactory.setPort(SettingsUtils.getPort());

  			JedisPoolConfig poolConfig = new JedisPoolConfig();
  			poolConfig.setMaxWaitMillis(2000L);
  			connectionFactory.setPoolConfig(poolConfig);
  			connectionFactory.afterPropertiesSet();

  			return connectionFactory;
      }
      
      @Bean
      RedisTemplate<?, ?> redisTemplate(RedisConnectionFactory connectionFactory) {
          RedisTemplate<byte[], byte[]> template = new RedisTemplate<>();
          template.setConnectionFactory(connectionFactory);
          return template;
      }
  	}
    
    @Autowired
    TestPersonRepository repo;
    
   @Test
    public void test_index_creation() {
        TestPerson p = new TestPerson();
        p.id = UUID.randomUUID();
        p.firstName = "my.first,name_" + p.id;
        String lastName = "my.last.name";
				p.lastName = lastName;
        p.address = new TestAddress();
        p.address.city = "Pleasanton";
        p.address.state = TestState.CA;
        p.address.streetNumber = 6220;
        p.address.street = "stoneridge mall rd";
        p.gender = TestGender.FEMALE;
        p.age = 25;
        repo.save(p);
        logger.info("after save: "+ p);
        Optional<TestPerson> findOne = repo.findById(p.id);
        assertNotNull(findOne);
        List<TestPerson> findByLastName = repo.findByFirstName(lastName);
        assertEquals(1, findByLastName.size());
    }
}
