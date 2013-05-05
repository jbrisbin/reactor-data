package reactor.data.spring;

import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.support.Repositories;

import java.io.Serializable;

import static org.springframework.core.GenericTypeResolver.resolveTypeArguments;

/**
 * @author Jon Brisbin
 */
public class ComposableRepositoryFactoryBean<R extends ComposableCrudRepository<T, ID>, T, ID extends Serializable>
		implements FactoryBean<R>,
							 BeanFactoryAware {

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

}
