package reactor.data.spring;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.*;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.support.Repositories;
import org.springframework.util.Assert;
import reactor.core.dynamic.reflect.MethodArgumentResolver;
import reactor.core.dynamic.reflect.MethodSelectorResolver;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.List;

import static org.springframework.core.GenericTypeResolver.resolveTypeArguments;

/**
 * @author Jon Brisbin
 */
public class ComposableRepositoryFactoryBean<R extends ComposableCrudRepository<T, ID>, T, ID extends Serializable>
		implements FactoryBean<R>,
							 BeanFactoryAware,
							 InitializingBean {

	private final Class<R>                        repositoryType;
	private       Class<? extends T>              domainType;
	private       ListableBeanFactory             beanFactory;
	private       Repositories                    repositories;
	private       List<MethodSelectorResolver>    methodSelectorResolvers;
	private       List<MethodArgumentResolver<?>> methodArgumentResolvers;
	private       CrudRepository<T, ID>           delegateRepository;
	private       R                               composableRepository;

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

	public List<MethodSelectorResolver> getMethodSelectorResolvers() {
		return methodSelectorResolvers;
	}

	public ComposableRepositoryFactoryBean<R, T, ID> setMethodSelectorResolvers(List<MethodSelectorResolver> methodSelectorResolvers) {
		Assert.notEmpty(methodSelectorResolvers, "MethodSelectorResolvers cannot be empty");
		this.methodSelectorResolvers = methodSelectorResolvers;
		return this;
	}

	public List<MethodArgumentResolver<?>> getMethodArgumentResolvers() {
		return methodArgumentResolvers;
	}

	public ComposableRepositoryFactoryBean<R, T, ID> setMethodArgumentResolvers(List<MethodArgumentResolver<?>> methodArgumentResolvers) {
		Assert.notEmpty(methodArgumentResolvers, "MethodArgumentResolvers cannot be empty");
		this.methodArgumentResolvers = methodArgumentResolvers;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
		if (beanFactory instanceof ListableBeanFactory) {
			this.beanFactory = (ListableBeanFactory) beanFactory;
			repositories = new Repositories(this.beanFactory);
			if (null != (delegateRepository = repositories.getRepositoryFor(domainType))) {
				ProxyFactory proxyFactory = new ProxyFactory(new SimpleComposableCrudRepository<T, ID>(delegateRepository));
				proxyFactory.addInterface(repositoryType);
				proxyFactory.addInterface(ComposableRepository.class);
				composableRepository = (R) proxyFactory.getProxy();
			}
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public void afterPropertiesSet() throws Exception {

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

	private static class ComposableRepositoryInvocationHandler implements InvocationHandler {
		final RepositoryInformation repoInfo;

		private ComposableRepositoryInvocationHandler(RepositoryInformation repoInfo) {
			this.repoInfo = repoInfo;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

			return null;
		}
	}

}
