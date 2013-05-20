package reactor.data.riak;

import com.basho.riak.client.bucket.Bucket;
import reactor.fn.Event;
import reactor.fn.Tuple2;

/**
 * @author Jon Brisbin
 */
public class DeleteEvent extends Event<Tuple2<Bucket, String>> {

	public DeleteEvent(Tuple2<Bucket, String> data) {
		super(data);
	}

}
