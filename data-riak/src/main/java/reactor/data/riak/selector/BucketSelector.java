package reactor.data.riak.selector;

import com.basho.riak.client.bucket.Bucket;
import reactor.fn.selector.BaseSelector;

/**
 * @author Jon Brisbin
 */
public class BucketSelector extends BaseSelector<Bucket> {

	public BucketSelector(Bucket bucket) {
		super(bucket);
	}

	@Override
	public boolean matches(Object key) {
		if (Bucket.class.isInstance(key)) {
			return getObject().getName().equals(((Bucket) key).getName());
		} else {
			return getObject().getName().equals(key);
		}
	}

}
