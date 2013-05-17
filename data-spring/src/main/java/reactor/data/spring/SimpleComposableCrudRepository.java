package reactor.data.spring;

import org.springframework.data.repository.CrudRepository;
import reactor.core.Composable;
import reactor.core.R;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Function;

import java.io.Serializable;

/**
 * @author Jon Brisbin
 */
class SimpleComposableCrudRepository<T, ID extends Serializable> implements ComposableCrudRepository<T, ID> {

	private final Reactor reactor = new Reactor();
	private final CrudRepository<T, ID> delegateRepository;

	SimpleComposableCrudRepository(CrudRepository<T, ID> delegateRepository) {
		this.delegateRepository = delegateRepository;
	}

	@Override

	public <S extends T> Composable<S> save(Composable<S> entities) {
		final Composable<S> c = new Composable<S>(reactor);
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
		return new Composable<ID>(reactor.getDispatcher())
				.map(new Function<ID, T>() {
					@Override
					public T apply(ID id) {
						return delegateRepository.findOne(id);
					}
				});
	}

	@Override
	public Composable<Boolean> exists(ID id) {
		return Composable.from(id)
										 .using(reactor)
										 .build()
										 .map(new Function<ID, Boolean>() {
											 @Override
											 public Boolean apply(ID id) {
												 return delegateRepository.exists(id);
											 }
										 });
	}

	@Override
	public Composable<T> findAll() {
		final Composable<T> c = new Composable<T>(reactor);
		Consumer<Void> consumer = new Consumer<Void>() {
			@Override
			public void accept(Void v) {
				for (T t : delegateRepository.findAll()) {
					c.accept(t);
				}
			}
		};
		R.schedule(consumer, null, reactor);
		return c;
	}

	@Override
	public Composable<T> findAll(Composable<ID> ids) {
		final Composable<T> c = new Composable<T>(reactor);
		ids.consume(new Consumer<ID>() {
			@Override
			public void accept(ID id) {
				c.accept(delegateRepository.findOne(id));
			}
		});
		return c;
	}

	@Override
	public Composable<Long> count() {
		final Composable<Long> c = new Composable<Long>();
		Consumer<Void> consumer = new Consumer<Void>() {
			@Override
			public void accept(Void v) {
				c.accept(delegateRepository.count());
			}
		};
		R.schedule(consumer, null, reactor);
		return c;
	}

	@Override
	public Composable<Void> delete(ID id) {
		return Composable.from(id)
										 .using(reactor)
										 .build()
										 .map(new Function<ID, Void>() {
											 @Override
											 public Void apply(ID id) {
												 delegateRepository.delete(id);
												 return null;
											 }
										 });
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Composable<Void> delete(Composable<? extends T> entities) {
		final Composable<Void> c = new Composable<Void>(reactor);
		entities.consume(new Consumer() {
			@Override
			public void accept(Object o) {
				delegateRepository.delete((T) o);
				c.accept((Void) null);
			}
		});
		return c;
	}

	@Override
	public Composable<Void> deleteAll() {
		final Composable<Void> c = new Composable<Void>(reactor);
		Consumer<Void> consumer = new Consumer<Void>() {
			@Override
			public void accept(Void aVoid) {
				delegateRepository.deleteAll();
				c.accept((Void) null);
			}
		};
		R.schedule(consumer, null, reactor);
		return c;
	}

}
