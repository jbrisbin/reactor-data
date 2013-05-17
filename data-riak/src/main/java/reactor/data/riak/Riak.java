package reactor.data.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.operations.StoreObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CachingRegistry;
import reactor.core.Composable;
import reactor.core.Reactor;
import reactor.data.riak.event.DeleteEvent;
import reactor.data.riak.event.FetchEvent;
import reactor.data.riak.event.MergeEvent;
import reactor.data.riak.event.StoreEvent;
import reactor.data.riak.selector.BucketSelector;
import reactor.fn.Function;
import reactor.fn.Registration;
import reactor.fn.Registry;
import reactor.fn.Tuple;
import reactor.fn.dispatch.Dispatcher;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Jon Brisbin
 */
public class Riak extends Reactor {

	private final Logger           log            = LoggerFactory.getLogger(Riak.class);
	private final Registry<Bucket> bucketRegistry = new CachingRegistry<>(null, null);
	private final IRiakClient riakClient;

	public Riak(IRiakClient riakClient) {
		this.riakClient = riakClient;
	}

	public Riak(IRiakClient riakClient, Dispatcher dispatcher) {
		super(dispatcher);
		this.riakClient = riakClient;
	}

	public Composable<Bucket> fetchBucket(String bucketName) {
		Iterator<Registration<? extends Bucket>> buckets = bucketRegistry.select(bucketName).iterator();
		if (!buckets.hasNext()) {
			return Composable.from(bucketName).using(getDispatcher()).build().
					map(name -> {
						try {
							Bucket bucket = riakClient.fetchBucket(name).execute();
							if (null == bucket) {
								throw new IllegalArgumentException("Bucket named " + name + " not found.");
							} else {
								BucketSelector bucketSel = new BucketSelector(bucket);
								bucketRegistry.register(bucketSel, bucket);
								if (log.isTraceEnabled()) {
									log.trace("Fetched bucket {}", bucket);
								}
								return bucket;
							}
						} catch (RiakRetryFailedException e) {
							throw new IllegalStateException(e);
						}
					});
		} else {
			return Composable.from(buckets.next().getObject()).using(getDispatcher()).build();
		}
	}

	public <T, O extends RiakOperation<T>> Composable<T> send(Composable<O> operation) {
		return operation.map(op -> {
			try {
				T obj = op.execute();
				if (log.isTraceEnabled()) {
					log.trace("{} resulted in {}", op, obj);
				}
				return obj;
			} catch (RiakException e) {
				throw new IllegalArgumentException(e);
			}
		});

	}

	public <T, O extends RiakOperation<T>> Composable<T> send(O operation) {
		return send(Composable.from(operation).using(getDispatcher()).build());
	}

	public <T> Composable<T> store(String bucketName, T value) {
		return store(bucketName, null, value, null, null, null);
	}

	public <T> Composable<T> store(String bucketName, String key,
																 T value) {
		return store(bucketName, key, value, null, null, null);
	}

	public <T> Composable<T> store(String bucketName, String key, T value, Function<Collection<T>, T> conflictResolver) {
		return store(bucketName, key, value, conflictResolver, null, null);
	}

	public <T> Composable<T> store(String bucketName,
																 String key,
																 T value,
																 Function<Collection<T>, T> conflictResolver,
																 Converter<T> converter,
																 Mutation<T> mutation) {
		String uri = "/" + bucketName + "/" + key;
		return send(
				fetchBucket(bucketName).
						map(bucket -> {
							StoreObject<T> op = (null == key ? bucket.store(value) : bucket.store(key, value));
							if (null != conflictResolver) {
								op = op.withResolver(siblings -> {
									T result = conflictResolver.apply(siblings);
									Riak.this.notify(uri, new MergeEvent<T>(Tuple.of(siblings, result)));
									return result;
								});
							}
							if (null != converter) {
								op = op.withConverter(converter);
							}
							if (null != mutation) {
								op = op.withMutator(mutation);
							}
							return op;
						}).
						consume(op -> Riak.this.notify(uri, new StoreEvent<>(op)))
		);
	}

	public <T> Composable<T> fetch(String bucketName, String key, Class<T> asType) {
		return fetch(bucketName, key, asType, null, null);
	}

	@SuppressWarnings("unchecked")
	public <T> Composable<T> fetch(String bucketName,
																 String key,
																 Class<T> asType,
																 Function<Collection<T>, T> conflictResolver,
																 Converter<T> converter) {
		String uri = "/" + bucketName + "/" + key;
		return send(
				fetchBucket(bucketName).
						map(bucket -> {
							FetchObject<T> op = null == asType ? (FetchObject<T>) bucket.fetch(key) : bucket.fetch(key, asType);
							if (null != conflictResolver) {
								op = op.withResolver(siblings -> {
									T result = conflictResolver.apply(siblings);
									Riak.this.notify(uri, new MergeEvent<T>(Tuple.of(siblings, result)));
									return result;
								});
							}
							if (null != converter) {
								op = op.withConverter(converter);
							}
							return op;
						}).
						consume(op -> Riak.this.notify(uri, new FetchEvent<>(op)))
		);
	}

	@SuppressWarnings("unchecked")
	public Composable<IRiakObject> fetch(String bucketName, String key) {
		return fetch(bucketName, key, null, null, null);
	}

	@SuppressWarnings("unchecked")
	public Composable<IRiakObject> fetch(String bucketName,
																			 String key,
																			 Function<Collection<IRiakObject>, IRiakObject> conflictResolver,
																			 Converter<IRiakObject> converter) {
		return fetch(bucketName, key, null, conflictResolver, converter);
	}

	public Composable<Void> delete(String bucketName, String key) {
		return delete(bucketName, key, null);
	}

	public Composable<Void> delete(String bucketName, String key, Retrier retrier) {
		String uri = "/" + bucketName + "/" + key;
		return send(
				fetchBucket(bucketName).
						map(bucket -> {
							DeleteObject op = bucket.delete(key);
							if (null != retrier) {
								op = op.withRetrier(retrier);
							}
							return op;
						}).
						consume(op -> Riak.this.notify(uri, new DeleteEvent(op)))
		);
	}

}
