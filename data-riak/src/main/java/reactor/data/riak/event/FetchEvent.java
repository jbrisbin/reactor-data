package reactor.data.riak.event;

import com.basho.riak.client.operations.FetchObject;
import reactor.fn.Event;

/**
 * @author Jon Brisbin
 */
public class FetchEvent<T> extends Event<FetchObject<T>> {

	public FetchEvent(FetchObject<T> op) {
		super(op);
	}

}
