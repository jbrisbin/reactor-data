package reactor.data.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.operations.DeleteObject;
import com.basho.riak.client.operations.StoreObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Composable;
import reactor.core.Promise;
import reactor.fn.Deferred;
import reactor.fn.Event;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static reactor.Fn.U;

/**
 * @author Jon Brisbin
 */
public class RiakTests {

	static final Logger LOG      = LoggerFactory.getLogger(RiakTests.class);
	static final long   objCount = 1000;

	long start;
	long end;
	long elapsed;
	long throughput;

	Riak riak;

	static <T> T await(Deferred<T> d, long seconds) throws InterruptedException {
		long start = System.currentTimeMillis();
		T result = d.await(seconds, TimeUnit.SECONDS);
		long end = System.currentTimeMillis();
		assertThat("await hasn't timed out", (end - start), lessThan(seconds * 1000));
		return result;
	}

	private void startTimer() {
		start = System.currentTimeMillis();
	}

	private void endTimer(String prefix) {
		end = System.currentTimeMillis();
		elapsed = end - start;
		throughput = Math.round((objCount / (elapsed * 1.0 / 1000)));
		LOG.info(prefix + " " + throughput + "/sec in " + elapsed + "ms");
	}

	@Before
	public void setup() throws RiakException {
		riak = new Riak();
	}

	@Test
	public void canFetchBucket() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		riak.fetchBucket("test").
				onSuccess(bucket -> latch.countDown());

		latch.await(5, TimeUnit.SECONDS);

		assertThat("latch was counted down", latch.getCount(), is(0L));
	}

	@Test
	public void canSendOperationsToRiak() throws InterruptedException {
		Promise<Bucket> p = riak.fetchBucket("test");
		Bucket b = await(p, 5);
		assertThat("bucket was retrieved", b, is(notNullValue()));

		Promise<IRiakObject> pObj = riak.send(b.fetch("test"));
		await(pObj, 5);
		assertThat("object was retrieved", pObj.get(), is(notNullValue()));
	}

	@Test
	public void canStoreData() throws InterruptedException {
		Bucket b = riak.fetchBucket("test").await(1, TimeUnit.SECONDS);
		assertThat("bucket was retrieved", b, is(notNullValue()));

		String s = riak.store(b, "test", "Hello World!", null, null, null).await(1, TimeUnit.SECONDS);
		LOG.info("store: " + s);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void canRespondToEvents() throws InterruptedException {
		Bucket b = riak.fetchBucket("test").await(1, TimeUnit.SECONDS);
		assertThat("bucket was retrieved", b, is(notNullValue()));

		CountDownLatch latch = new CountDownLatch(1);

		riak.on(U("/test/{key}"), (Event ev) -> {
			String key = ev.getHeaders().get("key");
			boolean isStore = StoreEvent.class.isInstance(ev);

			// Only count down on store
			if (isStore) {
				latch.countDown();
			}
		});

		riak.delete(b, "test", null).await(1, TimeUnit.SECONDS);

		assertThat("latch has not counted down", latch.getCount(), is(1L));

		riak.store(b, "test", "Hello World!", null, null, null).await(1, TimeUnit.SECONDS);
		latch.await(5, TimeUnit.SECONDS);

		assertThat("latch has counted down", latch.getCount(), is(0L));
	}

	@Test
	public void canFetchUseCallbacks() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		riak.fetchBucket("test").onSuccess(
				bucket -> {
					StoreObject<IRiakObject> storeOp = bucket.store("test", "Hello World!");
					riak.send(storeOp).onSuccess(
							obj -> latch.countDown()
					);
				}
		);

		latch.await(5, TimeUnit.SECONDS);
		assertThat("latch is counted down", latch.getCount(), is(0L));
	}

	@Test
	public void canMap() throws InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		Composable<String> cStr = riak.fetchBucket("test").map(
				bucket -> {
					latch.countDown();
					return bucket.getName();
				}
		);

		String s = cStr.await(1, TimeUnit.SECONDS);
		assertThat("value is returned", s, is("test"));
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void riakCanSupportLargeVolumesOfWrites() throws InterruptedException, RiakException {
		Bucket test = riak.fetchBucket("test").await(1, TimeUnit.SECONDS);

		CountDownLatch latch = new CountDownLatch(2);

		LOG.info("Cleaning {} documents...", objCount);
		DeleteObject[] deleteOps = new DeleteObject[(int) objCount];
		for (int i = 0; i < objCount; i++) {
			deleteOps[i] = test.delete("test" + i);
		}
		riak.send(deleteOps).onSuccess(v -> latch.countDown());
		LOG.info("Done cleaning documents.", objCount);

		// Let Riak catch up
		Thread.sleep(5000);

		LOG.info("Starting timed store of {} documents...", objCount);
		startTimer();
		StoreObject[] storeOps = new StoreObject[(int) objCount];
		for (int i = 0; i < objCount; i++) {
			storeOps[i] = test.store("test" + i, "Hello World!");
		}
		endTimer("throughput for queue:");

		riak.send(storeOps).onSuccess(v -> latch.countDown());
		latch.await(30, TimeUnit.SECONDS);

		endTimer("throughput for wait:");

		// Let Riak catch up
		Thread.sleep(5000);

		String s = riak.fetch(test, "test" + (objCount - 1), String.class, null, null).await(1, TimeUnit.SECONDS);
		assertThat("document was stored", s, is("Hello World!"));
	}

}
