package reactor.data.spring;

import reactor.core.Composable;

import java.io.Serializable;

/**
 * @author Jon Brisbin
 */
public interface ComposableCrudRepository<T, ID extends Serializable> extends ComposableRepository<T, ID> {

	/**
	 * Saves all given entities.
	 *
	 * @param entities
	 * @return the saved entities
	 * @throws IllegalArgumentException in case the given entity is (@literal null}.
	 */
	<S extends T> Composable<S> save(Composable<S> entities);

	/**
	 * Retrieves an entity by its id.
	 *
	 * @param id must not be {@literal null}.
	 * @return the entity with the given id or {@literal null} if none found
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	Composable<T> findOne(ID id);

	/**
	 * Returns whether an entity with the given id exists.
	 *
	 * @param id must not be {@literal null}.
	 * @return true if an entity with the given id exists, {@literal false} otherwise
	 * @throws IllegalArgumentException if {@code id} is {@literal null}
	 */
	Composable<Boolean> exists(ID id);

	/**
	 * Returns all instances of the type.
	 *
	 * @return all entities
	 */
	Composable<T> findAll();

	/**
	 * Returns all instances of the type with the given IDs.
	 *
	 * @param ids
	 * @return
	 */
	Composable<T> findAll(Composable<ID> ids);

	/**
	 * Returns the number of entities available.
	 *
	 * @return the number of entities
	 */
	Composable<Long> count();

	/**
	 * Deletes the entity with the given id.
	 *
	 * @param id must not be {@literal null}.
	 * @throws IllegalArgumentException in case the given {@code id} is {@literal null}
	 */
	Composable<Void> delete(ID id);

	/**
	 * Deletes the given entities.
	 *
	 * @param entities
	 * @throws IllegalArgumentException in case the given {@link Iterable} is (@literal null}.
	 */
	Composable<Void> delete(Composable<? extends T> entities);

	/**
	 * Deletes all entities managed by the repository.
	 */
	Composable<Void> deleteAll();

}
