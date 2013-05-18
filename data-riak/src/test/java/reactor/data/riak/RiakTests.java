package reactor.data.riak;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import org.junit.Before;
import org.junit.Test;
import reactor.core.Composable;
import reactor.core.Promise;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * @author Jon Brisbin
 */
public class RiakTests {

	static final long objCount = 2000;

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
		System.out.println("starting timer...");
		start = System.currentTimeMillis();
	}

	private void endTimer(String prefix) {
		end = System.currentTimeMillis();
		elapsed = end - start;
		throughput = Math.round((objCount / (elapsed * 1.0 / 1000)));
		System.out.println(prefix + " " + throughput + "/s in " + elapsed + "ms");
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
		CountDownLatch latch = new CountDownLatch(1);

		Bucket b = riak.fetchBucket("test").await();
		assertThat("bucket was retrieved", b, is(notNullValue()));

		IRiakObject iro  = riak.send(b.fetch("test")).await();
		assertThat("object was retrieved", iro, is(notNullValue()));

	}

}
