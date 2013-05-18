package reactor.data.riak;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.operations.RiakOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.core.CachingRegistry;
import reactor.core.Promise;
import reactor.core.Reactor;
import reactor.data.riak.selector.BucketSelector;
import reactor.fn.*;
import reactor.fn.dispatch.Dispatcher;

import java.util.Iterator;

import static reactor.Fn.$;
import static reactor.Fn.U;
import static reactor.core.Context.nextWorkerDispatcher;
import static reactor.core.Context.threadPoolDispatcher;

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
	private final Reactor     reactor;

	public Riak(IRiakClient riakClient) {
		this(riakClient, nextWorkerDispatcher());
	}

	public Riak(IRiakClient riakClient, Dispatcher dispatcher) {
		this.riakClient = riakClient;
		this.reactor = new Reactor(dispatcher);
		subscribeOperationConsumers();
	}

	public <T, O extends RiakOperation<T>> Promise<T> send(O op) {
		Promise<T> p = new Promise<>(reactor);
		reactor.notify("op", Fn.event(Tuple.of(op, p)));
		return p;
	}

	public Promise<Bucket> fetchBucket(String name) {
		Promise<Bucket> p = new Promise<>(reactor);
		reactor.notify("/" + name + "/fetch", Fn.event(p));
		return p;
	}

	private void subscribeOperationConsumers() {
		reactor.on(U("/{bucket}/fetch"), (Event<Promise<Bucket>> ev) -> {
			String bucketName = ev.getHeaders().get("bucket");
			Promise<Bucket> promise = ev.getData();

			Iterator<Registration<? extends Bucket>> buckets = bucketRegistry.select(bucketName).iterator();
			if (!buckets.hasNext()) {

				Bucket b = null;
				try {
					b = riakClient.fetchBucket(bucketName).execute();
					bucketRegistry.register(new BucketSelector(b), b);
					if (log.isTraceEnabled()) {
						log.trace("{} fetched", b);
					}
					promise.set(b);
				} catch (Throwable t) {
					promise.set(t);
				}

			} else {
				promise.set(buckets.next().getObject());
			}
		});

		reactor.on($("op"), (Event<Tuple2<RiakOperation<Object>, Promise<Object>>> ev) -> {
			try {
				RiakOperation<Object> op = ev.getData().getT1();
				Promise<Object> p = ev.getData().getT2();
				Object result = op.execute();
				if (log.isTraceEnabled()) {
					log.trace("{} result: {}", op, result);
				}
				p.set(result);
			} catch (RiakException e) {
				throw new IllegalStateException(e);
			}
		});
	}

}
