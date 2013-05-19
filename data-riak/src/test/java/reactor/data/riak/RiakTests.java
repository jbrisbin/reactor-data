package reactor.data.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.operations.DeleteObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Composable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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

	static <T> T await(Composable<T> composable, long seconds) throws InterruptedException {
		long start = System.currentTimeMillis();
		T result = composable.await(seconds, TimeUnit.SECONDS);
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
		riak = new Riak(RiakFactory.pbcClient());
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
		Bucket b = riak.fetchBucket("test").await();
		assertThat("bucket was retrieved", b, is(notNullValue()));

		IRiakObject iro = riak.send(b.fetch("test")).await();
		assertThat("object was retrieved", iro, is(notNullValue()));
	}

	@Test
	public void canStoreData() throws InterruptedException {
		Bucket b = riak.fetchBucket("test").await();
		assertThat("bucket was retrieved", b, is(notNullValue()));

		String s = riak.store(b, "test", "Hello World!", null, null, null).await();
		LOG.info("store: " + s);
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void riakCanSupportLargeVolumesOfWrites() throws InterruptedException, RiakException {
		Bucket test = riak.fetchBucket("test").await(1, TimeUnit.SECONDS);
		LOG.info("Cleaning {} documents...", objCount);
		DeleteObject[] ops = new DeleteObject[(int) objCount];
		for (int i = 0; i < objCount; i++) {
			ops[i] = test.delete("test" + i);
		}
		riak.send(ops).await();
		LOG.info("Done cleaning documents.", objCount);

		CountDownLatch latch = new CountDownLatch((int) objCount);

		LOG.info("Starting timed store of {} documents...", objCount);
		startTimer();
		for (int i = 0; i < objCount; i++) {
			riak.store(test, "test" + i, "Hello World!", null, null, null).
					onSuccess(data -> latch.countDown());
		}
		endTimer("throughput for queue:");

		latch.await(30, TimeUnit.SECONDS);

		endTimer("throughput for wait:");
	}

}
