# Reactor Data

This is an early draft of enabling Reactor's Composable to provide asynchronous data access even if the underlying datastore access is fully-blocking.

### Spring Data Repositories

This submodule provides an interface similar to Spring Data's [CrudRepository](http://static.springsource.org/spring-data/commons/docs/current/api/org/springframework/data/repository/CrudRepository.html) but replaces references to domain objects with references to `Composable<DomainObject>`. This gives you a chainable, composable data API that doesn't block the caller while waiting on data (it uses a background IO thread instead).

Given a domain object called `Person`, and a Spring Data Repository to manage it, you just have to declare a subclass of `ComposableCrudRepository` and you will get a fluent composable API that lets you do map, filter, and other composable actions like the following silly example:

    @Inject
    ComposablePersonRepository people;

    Composable<String> fullName = people.findOne(personId).map(new Function<Person, String>() {
      public String apply(Person p) {
        return p.getFirstName() + " " + p.getLastName();
      }
    });

You can block if you want by calling Composable's `await(long timeout, TimeUnit unit)` method. Or you can do a non-blocking call to `get()` (`Composable<T>`s are also `Supplier<T>`s).

    String s = fullName.await(5, TimeUnit.SECONDS); // block caller for 5 seconds


