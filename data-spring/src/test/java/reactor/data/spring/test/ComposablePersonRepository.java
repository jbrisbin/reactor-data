package reactor.data.spring.test;

import reactor.core.Composable;
import reactor.data.spring.ComposableCrudRepository;

/**
 * @author Jon Brisbin
 */
public interface ComposablePersonRepository extends ComposableCrudRepository<Person, Long> {

	Composable<Person> findByName(String name);

}
