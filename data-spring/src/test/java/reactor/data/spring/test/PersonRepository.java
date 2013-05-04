package reactor.data.spring.test;

import org.springframework.data.repository.CrudRepository;

/**
 * @author Jon Brisbin
 */
public interface PersonRepository extends CrudRepository<Person, Long> {
}
