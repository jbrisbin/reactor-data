package reactor.data.riak.event;

import com.basho.riak.client.operations.StoreObject;
import reactor.fn.Event;

/**
 * @author Jon Brisbin
 */
public class StoreEvent<T> extends Event<StoreObject<T>> {

	public StoreEvent(StoreObject<T> op) {
		super(op);
	}

}
