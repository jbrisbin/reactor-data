package reactor.data.spring.config;

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.ClassUtils;
import reactor.data.spring.ComposableRepository;
import reactor.data.spring.ComposableRepositoryFactoryBean;

import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class ComposableRepositoryBeanDefinitionRegistrar
		implements ImportBeanDefinitionRegistrar,
							 ResourceLoaderAware {

	private final ClassLoader classLoader = getClass().getClassLoader();
	private ResourceLoader resourceLoader;

	@Override
	public void setResourceLoader(ResourceLoader resourceLoader) {
		this.resourceLoader = resourceLoader;
	}

	@Override
	public void registerBeanDefinitions(AnnotationMetadata meta, BeanDefinitionRegistry registry) {

		Map<String, Object> attrs = meta.getAnnotationAttributes(EnableComposableRepositories.class.getName());

		ClassPathScanningCandidateComponentProvider provider = new ClassPathScanningCandidateComponentProvider(false) {
			@Override
			protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
				return beanDefinition.getMetadata().isIndependent();
			}
		};
		provider.addIncludeFilter(new AssignableTypeFilter(ComposableRepository.class));
		provider.setResourceLoader(resourceLoader);

		String[] basePackages = (String[]) attrs.get("basePackages");
		if (basePackages.length == 0) {
			String s = "";
			try {
				s = Class.forName(meta.getClassName()).getPackage().getName();
			} catch (ClassNotFoundException e) {
			}
			basePackages = new String[]{s};
		}

		for (String basePackage : basePackages) {
			for (BeanDefinition beanDef : provider.findCandidateComponents(basePackage)) {
				BeanDefinitionBuilder factoryBeanDef = BeanDefinitionBuilder.rootBeanDefinition(ComposableRepositoryFactoryBean.class.getName());
				factoryBeanDef.addConstructorArgValue(ClassUtils.resolveClassName(beanDef.getBeanClassName(), classLoader));

				registry.registerBeanDefinition(beanDef.getBeanClassName(), factoryBeanDef.getBeanDefinition());
			}
		}
	}

}
