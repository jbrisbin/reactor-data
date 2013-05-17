package reactor.data.riak;

import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.operations.StoreObject;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Composable;
import reactor.core.Context;
import reactor.data.riak.event.StoreEvent;
import reactor.fn.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static reactor.Fn.U;

/**
 * @author Jon Brisbin
 */
public class RiakTests {

	static final long objCount = 500;

	Riak riak;

	static <T> T await(Composable<T> composable, long seconds) throws InterruptedException {
		long start = System.currentTimeMillis();
		T result = composable.await(seconds, TimeUnit.SECONDS);
		long end = System.currentTimeMillis();
		assertThat("await hasn't timed out", (end - start), lessThan(seconds * 1000));
		return result;
	}

	@Before
	public void setup() throws RiakException {
		riak = new Riak(RiakFactory.pbcClient(), Context.nextWorkerDispatcher());
	}

	@Test
	public void riakCanStoreData() throws InterruptedException {
		Composable<?> store = riak.store("test", "test", "Hello World!");
		await(store, 5); // wait for store to complete

		Composable<String> fetch = riak.fetch("test", "test", String.class);
		String hello = await(fetch, 5); // wait for fetch to complete

		assertThat("result was fetched asynchronously", hello, is("Hello World!"));
	}

	@Test
	public void riakCanDeleteData() throws InterruptedException {
		Composable<?> store = riak.store("test", "test", "Hello World!");
		await(store, 5); // wait for store to complete

		Composable<Void> delete = riak.delete("test", "test");
		await(delete, 5); // wait for delete to complete

		Composable<String> fetch = riak.fetch("test", "test", String.class);
		String hello = await(fetch, 5); // wait for fetch to complete

		assertThat("result was fetched asynchronously", hello, is(nullValue()));
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void riakCanNotifyConsumersOfOperations() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(2);

		riak.on(U("/test/{key}"), (Event ev) -> {
			// only count down on stores
			if (StoreEvent.class.isInstance(ev)) {
				latch.countDown();
			}
		});

		Composable<?> store = riak.store("test", "test", "Hello World!");
		await(store, 5); // wait for store to complete

		Composable<String> fetch = riak.fetch("test", "test", String.class);
		String hello = await(fetch, 5); // wait for fetch to complete

		assertThat("result was fetched asynchronously", hello, is("Hello World!"));

		store = riak.store("test", "test", "Hello World!");
		await(store, 5); // wait for store to complete

		latch.await(5, TimeUnit.SECONDS);
		assertThat("latch was counted down", latch.getCount(), is(0L));
	}

	@Test
	public void riakCanSupportLargeVolumesOfWrites() throws InterruptedException {
		List<StoreObject<?>> ops = new ArrayList<>((int) objCount);
		List<Composable<?>> opWaits = new ArrayList<>((int) objCount);

		Composable<Bucket> bucket = riak.fetchBucket("test");
		await(bucket, 1);

		for (int i = 0; i < objCount; i++) {
			ops.add(bucket.get().store("test" + i, "Hello World!".getBytes()));
		}

		long start = System.currentTimeMillis();
		for (StoreObject<?> op : ops) {
			opWaits.add(riak.send(op));
		}
		long end = System.currentTimeMillis();
		long elapsed = (end - start);

		System.out.println("throughput to queue: " + Math.round((objCount / (elapsed * 1.0 / 1000))) + "/s in " + elapsed + "ms");

		for (Composable<?> wait : opWaits) {
			await(wait, 1);
		}
		end = System.currentTimeMillis();
		elapsed = (end - start);

		System.out.println("throughput to wait: " + Math.round((objCount / (elapsed * 1.0 / 1000))) + "/s in " + elapsed + "ms");
	}

}
