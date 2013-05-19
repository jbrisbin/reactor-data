package reactor.data.riak;

import reactor.fn.Event;
import reactor.fn.Tuple2;

import java.util.Collection;

/**
 * @author Jon Brisbin
 */
public class MergeEvent<T> extends Event<Tuple2<Collection<T>, T>> {

	public MergeEvent(Tuple2<Collection<T>, T> data) {
		super(data);
	}

}
