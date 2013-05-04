package reactor.data.spring;

import org.springframework.data.repository.CrudRepository;
import reactor.core.Composable;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.io.Serializable;

/**
 * @author Jon Brisbin
 */
class SimpleComposableCrudRepository<T, ID extends Serializable> implements ComposableCrudRepository<T, ID> {

	private final CrudRepository<T, ID> delegateRepository;

	SimpleComposableCrudRepository(CrudRepository<T, ID> delegateRepository) {
		this.delegateRepository = delegateRepository;
	}

	@Override

	public <S extends T> Composable<S> save(Composable<S> entities) {
		final Composable<S> c = new Composable<S>();
		entities.consume(new Consumer<S>() {
			@Override
			public void accept(S entity) {
				c.accept(delegateRepository.save(entity));
			}
		});
		return c;
	}

	@Override
	public Composable<T> findOne(ID id) {
		return Composable.from(id).map(new Function<ID, T>() {
			@Override
			public T apply(ID id) {
				return delegateRepository.findOne(id);
			}
		});
	}

	@Override
	public Composable<Boolean> exists(ID id) {
		return Composable.from(id).map(new Function<ID, Boolean>() {
			@Override
			public Boolean apply(ID id) {
				return delegateRepository.exists(id);
			}
		});
	}

	@Override
	public Composable<T> findAll() {
		final Composable<T> c = new Composable<T>();
		return c;
	}

	@Override
	public Composable<T> findAll(Composable<ID> ids) {
		return null;
	}

	@Override
	public Composable<Long> count() {
		return null;
	}

	@Override
	public void delete(ID id) {
	}

	@Override
	public void delete(Composable<? extends T> entities) {
	}

	@Override
	public void deleteAll() {
	}

}
