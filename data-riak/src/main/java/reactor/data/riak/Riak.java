package reactor.data.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.Mutation;
import com.basho.riak.client.cap.Retrier;
import com.basho.riak.client.convert.Converter;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.FetchObject;
import com.basho.riak.client.operations.RiakOperation;
import com.basho.riak.client.operations.StoreObject;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.CachingRegistry;
import reactor.core.Promise;
import reactor.core.R;
import reactor.core.Reactor;
import reactor.data.riak.selector.BucketSelector;
import reactor.fn.Function;
import reactor.fn.Registration;
import reactor.fn.Registry;
import reactor.fn.Tuple;
import reactor.fn.dispatch.Dispatcher;
import reactor.fn.dispatch.RingBufferDispatcher;

import java.util.Collection;
import java.util.Iterator;

import static reactor.core.Context.rootDispatcher;

/**
 * Instances of this class manage the execution of {@link RiakOperation RiakOperations} so that the user doesn't
 * directly call {@link com.basho.riak.client.operations.RiakOperation#execute()}. All operations are asynchronous and
 * the Reactor "worker" {@link Dispatcher} is used to ensure that operational load for the IO is
 *
 * @author Jon Brisbin
 */
public class Riak {

	private final Logger           log            = LoggerFactory.getLogger(Riak.class);
	private final Registry<Bucket> bucketRegistry = new CachingRegistry<>(null, null);
	private final IRiakClient riakClient;
	private final Reactor     ioReactor;
	private final Reactor     reactor;

	public Riak() throws RiakException {
		this(RiakFactory.pbcClient());
	}

	public Riak(IRiakClient riakClient) {
		this(riakClient, rootDispatcher());
	}

	public Riak(IRiakClient riakClient, Dispatcher customDispatcher) {
		this.riakClient = riakClient;
		this.reactor = new Reactor(customDispatcher);
		this.ioReactor = new Reactor(new RingBufferDispatcher(
				"riak",
				1,
				1024,
				ProducerType.MULTI,
				new YieldingWaitStrategy()
		));
	}

	public Promise<Void> send(RiakOperation<?>... ops) {
		Promise<Void> p = new Promise<>(reactor);

		R.schedule(
				(Void v) -> {
					for (RiakOperation<?> op : ops) {
						try {
							op.execute();
						} catch (RiakException e) {
							p.set(e);
							break;
						}
					}
					p.set((Void) null);
				},
				null,
				ioReactor
		);

		return p;
	}

	public <T, O extends RiakOperation<T>> Promise<T> send(O op) {
		Promise<T> p = new Promise<>(reactor);

		R.schedule(
				(Void v) -> {
					try {
						T result = op.execute();
						if (log.isTraceEnabled()) {
							log.trace("{} result: {}", op, result);
						}
						p.set(result);
					} catch (RiakException e) {
						p.set(e);
					}
				},
				null,
				ioReactor
		);

		return p;
	}

	public Promise<Bucket> fetchBucket(String name) {
		Promise<Bucket> p = new Promise<>(reactor);

		Iterator<Registration<? extends Bucket>> buckets = bucketRegistry.select(name).iterator();
		if (!buckets.hasNext()) {
			R.schedule(
					(Void v) -> {
						try {
							Bucket b = riakClient.fetchBucket(name).execute();
							bucketRegistry.register(new BucketSelector(b), b);
							if (log.isTraceEnabled()) {
								log.trace("Fetched: {}", b);
							}
							p.set(b);
						} catch (Throwable t) {
							p.set(t);
						}
					},
					null,
					ioReactor
			);
		} else {
			p.set(buckets.next().getObject());
		}

		return p;
	}

	public <T> Promise<T> store(Bucket bucket,
															String key,
															T value,
															Function<Collection<T>, T> conflictResolver,
															Converter<T> converter,
															Mutation<T> mutation) {
		Promise<T> p = new Promise<>(reactor);

		R.schedule(
				(Void v) -> {
					StoreObject<T> op = (null == key ? bucket.store(value) : bucket.store(key, value));
					if (log.isTraceEnabled()) {
						log.trace("Preparing operation {}", op);
					}
					if (null != conflictResolver) {
						op = op.withResolver(siblings -> {
							T result = conflictResolver.apply(siblings);
							reactor.notify("/" + bucket.getName() + "/" + key, new MergeEvent<T>(Tuple.of(siblings, result)));
							return result;
						});
					}
					if (null != converter) {
						op = op.withConverter(converter);
					}
					if (null != mutation) {
						op = op.withMutator(mutation);
					}

					try {
						T result = op.execute();
						if (log.isTraceEnabled()) {
							log.trace("/{}/{} stored: {}", bucket.getName(), key, result);
						}
						reactor.notify("/" + bucket.getName() + "/" + key, new StoreEvent<>(result));

						p.set(result);
					} catch (RiakRetryFailedException e) {
						p.set(e);
					}
				},
				null,
				ioReactor
		);

		return p;
	}

	@SuppressWarnings({"unchecked"})
	public <T> Promise<T> fetch(Bucket bucket,
															String key,
															Class<T> asType,
															Function<Collection<T>, T> conflictResolver,
															Converter<T> converter) {
		Promise<T> p = new Promise<>(reactor);

		R.schedule(
				(Void v) -> {
					FetchObject<T> op = (null == asType ? (FetchObject<T>) bucket.fetch(key) : bucket.fetch(key, asType));
					if (null != conflictResolver) {
						op = op.withResolver(siblings -> {
							T result = conflictResolver.apply(siblings);
							reactor.notify("/" + bucket.getName() + "/" + key, new MergeEvent<T>(Tuple.of(siblings, result)));
							return result;
						});
					}
					if (null != converter) {
						op = op.withConverter(converter);
					}

					try {
						T result = op.execute();
						if (log.isTraceEnabled()) {
							log.trace("/{}/{} fetched: {}", bucket.getName(), key, result);
						}
						p.set(result);
					} catch (RiakRetryFailedException e) {
						p.set(e);
					}
				},
				null,
				ioReactor
		);
		return p;
	}

	public Promise<Void> delete(Bucket bucket,
															String key,
															Retrier retrier) {
		Promise<Void> p = new Promise<>(reactor);

		R.schedule(
				(Void v) -> {
					DeleteObject op = bucket.delete(key);
					if (null != retrier) {
						op = op.withRetrier(retrier);
					}
					try {
						Void result = op.execute();
						if (log.isTraceEnabled()) {
							log.trace("deleted: /{}/{}", bucket.getName(), key);
						}
						p.set(result);
					} catch (RiakException e) {
						p.set(e);
					}
				},
				null,
				ioReactor
		);

		return p;
	}
}
