package reactor.data.spring

import com.mongodb.Mongo
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories
import reactor.core.Composable
import reactor.data.spring.config.EnableComposableRepositories
import reactor.data.spring.test.ComposablePersonRepository
import reactor.data.spring.test.Person
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
class ComposableRepositorySpec extends Specification {

	AnnotationConfigApplicationContext appCtx

	def setup() {
		appCtx = new AnnotationConfigApplicationContext(SpecConfig)
	}

	def "generates proxies for ComposableRepositories"() {

		when: "a repository is requested"
		def repo = appCtx.getBean(ComposablePersonRepository)

		then: "a repository is provided"
		null != repo

	}

	def "provides repository methods that return Composables"() {

		given: "a ComposableRepository"
		def people = appCtx.getBean(ComposablePersonRepository)

		when: "an entity is saved"
		def start = System.currentTimeMillis()
		def entity = people.save(Composable.from(new Person(id: 1, name: "John Doe")))
		entity.await(1, TimeUnit.SECONDS)

		then: "entity has saved without timing out"
		System.currentTimeMillis() - start < 5000

		when: "an entity is requested"
		entity = people.findOne(1)

		then: "it should have a name"
		entity.await(1, TimeUnit.SECONDS)?.name == "John Doe"

	}

}

@Configuration
@EnableMongoRepositories(basePackages = ["reactor.data.spring.test"])
@EnableComposableRepositories(basePackages = ["reactor.data.spring.test"])
class SpecConfig {

	@Bean
	MongoTemplate mongoTemplate() {
		return new MongoTemplate(new Mongo(), "reactor")
	}

}
