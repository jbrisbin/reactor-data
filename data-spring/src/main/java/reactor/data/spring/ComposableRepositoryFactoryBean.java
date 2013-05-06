package reactor.data.spring;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.support.Repositories;
import org.springframework.util.ReflectionUtils;
import reactor.core.Composable;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.core.GenericTypeResolver.resolveTypeArguments;
import static org.springframework.util.ReflectionUtils.doWithMethods;

/**
 * @author Jon Brisbin
 */
public class ComposableRepositoryFactoryBean<R extends ComposableCrudRepository<T, ID>, T, ID extends Serializable>
		implements FactoryBean<R>,
							 ApplicationListener<ContextRefreshedEvent> {

	private final Class<R>              repositoryType;
	private       Class<? extends T>    domainType;
	private       ListableBeanFactory   beanFactory;
	private       Repositories          repositories;
	private       CrudRepository<T, ID> delegateRepository;
	private       R                     composableRepository;

	@SuppressWarnings("unchecked")
	public ComposableRepositoryFactoryBean(Class<R> repositoryType) {
		this.repositoryType = repositoryType;
		for (Class<?> intfType : repositoryType.getInterfaces()) {
			if (!ComposableRepository.class.isAssignableFrom(intfType)) {
				continue;
			}
			Class<?>[] types = resolveTypeArguments(repositoryType, ComposableRepository.class);
			this.domainType = (Class<? extends T>) types[0];
			break;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onApplicationEvent(ContextRefreshedEvent event) {
		if (null != composableRepository) {
			return;
		}
		this.beanFactory = event.getApplicationContext();
		repositories = new Repositories(this.beanFactory);
		if (null != (delegateRepository = repositories.getRepositoryFor(domainType))) {
			RepositoryInformation repoInfo = repositories.getRepositoryInformationFor(domainType);

			SimpleComposableCrudRepository<T, ID> repo = new SimpleComposableCrudRepository<T, ID>(delegateRepository);

			ProxyFactory proxyFactory = new ProxyFactory(repo);
			proxyFactory.addInterface(repositoryType);
			proxyFactory.addInterface(ComposableRepository.class);

			proxyFactory.addAdvice(
					new QueryMethodExecutor<R, T, ID>(
							repoInfo,
							repositoryType,
							repo,
							delegateRepository
					)
			);

			composableRepository = (R) proxyFactory.getProxy();
		}
	}

	@Override
	public R getObject() throws Exception {
		return composableRepository;
	}

	@Override
	public Class<R> getObjectType() {
		return repositoryType;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

	private static class QueryMethodExecutor<R extends ComposableCrudRepository<T, ID>, T, ID extends Serializable> implements MethodInterceptor {
		private final Map<String, Method>     methods      = new HashMap<String, Method>();
		private final Map<String, Method>     queryMethods = new HashMap<String, Method>();
		private final Map<String, Class<?>[]> paramTypes   = new HashMap<String, Class<?>[]>();
		private final Class<R>                        composableRepositoryType;
		private final ComposableCrudRepository<T, ID> composableRepository;
		private final CrudRepository<T, ID>           delegateRepository;
		private final RepositoryInformation           repoInfo;

		private QueryMethodExecutor(final RepositoryInformation repoInfo,
																Class<R> composableRepositoryType,
																ComposableCrudRepository<T, ID> composableRepository,
																CrudRepository<T, ID> delegateRepository) {
			this.repoInfo = repoInfo;
			this.composableRepositoryType = composableRepositoryType;
			this.composableRepository = composableRepository;
			this.delegateRepository = delegateRepository;

			doWithMethods(
					composableRepositoryType,
					new ReflectionUtils.MethodCallback() {
						@Override
						public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
							String name = method.toGenericString();
							Class<?>[] paramTypes = method.getParameterTypes();
							QueryMethodExecutor.this.paramTypes.put(name, paramTypes);
						}
					},
					new ReflectionUtils.MethodFilter() {
						@Override
						public boolean matches(Method method) {
							return Object.class != method.getDeclaringClass() && !method.getName().contains("$");
						}
					}
			);
		}

		@SuppressWarnings("unchecked")
		@Override
		public Object invoke(MethodInvocation invocation) throws Throwable {
			String name = invocation.getMethod().toGenericString();
			Class<?>[] paramTypes = this.paramTypes.get(name);

			try {
				Method m = methods.get(name);
				if (null == m) {
					m = invocation.getThis().getClass().getDeclaredMethod(invocation.getMethod().getName(), paramTypes);
					if (null != m) {
						methods.put(name, m);
					}
				}

				if (null == m) {
					throw new NoSuchMethodError(name);
				}

				System.out.println("method: " + m);
				System.out.println("param: " + paramTypes.length);

				return m.invoke(invocation.getThis(), invocation.getArguments());
			} catch (Exception e) {
				if (NoSuchMethodException.class.isAssignableFrom(e.getClass())) {
					// this is probably a finder method
					Method m = queryMethods.get(name);
					if (null == m) {
						m = delegateRepository.getClass().getDeclaredMethod(invocation.getMethod().getName(), paramTypes);
						if (null != m) {
							Object result = m.invoke(delegateRepository, invocation.getArguments());
							if (result instanceof Iterable) {
								return Composable.from((Iterable) result);
							} else {
								return Composable.from(result);
							}
						}
						queryMethods.put(name, m);
					}
				}
			}

			throw new NoSuchMethodException(name);
		}
	}

}
