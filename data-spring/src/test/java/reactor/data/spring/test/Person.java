package reactor.data.spring.test;

import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author Jon Brisbin
 */
@Document
public class Person {

	private Long   id;
	private String name;

}
