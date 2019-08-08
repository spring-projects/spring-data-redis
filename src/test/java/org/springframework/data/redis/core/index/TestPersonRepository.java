package org.springframework.data.redis.core.index;

import java.util.List;
import java.util.UUID;

import org.springframework.data.repository.CrudRepository;

interface TestPersonRepository extends CrudRepository<TestPerson, UUID> {

	List<TestPerson> findByLastName(String lastname);
	List<TestPerson> findByAddress_City(String city);
	List<TestPerson> findByFirstName(String firstName);
}
