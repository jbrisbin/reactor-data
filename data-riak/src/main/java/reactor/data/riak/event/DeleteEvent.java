package reactor.data.riak.event;

import com.basho.riak.client.operations.DeleteObject;
import reactor.fn.Event;

/**
 * @author Jon Brisbin
 */
public class DeleteEvent extends Event<DeleteObject> {

	public DeleteEvent(DeleteObject op) {
		super(op);
	}

}
