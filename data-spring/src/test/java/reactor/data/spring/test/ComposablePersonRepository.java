package reactor.data.spring.test;

import reactor.data.spring.ComposableCrudRepository;

/**
 * @author Jon Brisbin
 */
public interface ComposablePersonRepository extends ComposableCrudRepository<Person, Long> {
}
