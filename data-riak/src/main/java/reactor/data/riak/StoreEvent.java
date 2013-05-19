package reactor.data.riak;

import reactor.fn.Event;

/**
 * @author Jon Brisbin
 */
public class StoreEvent<T> extends Event<T> {

	public StoreEvent(T data) {
		super(data);
	}

}
