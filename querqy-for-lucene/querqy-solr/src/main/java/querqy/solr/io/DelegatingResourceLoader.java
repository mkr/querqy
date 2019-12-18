package querqy.solr.io;

import org.apache.lucene.analysis.util.ResourceLoader;

import java.io.IOException;
import java.io.InputStream;

public class DelegatingResourceLoader implements ResourceLoader {

    private ResourceLoader delegate;

    @Override
    public InputStream openResource(String resource) throws IOException {
        return delegate.openResource(resource);
    }

    @Override
    public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
        return delegate.findClass(cname, expectedType);
    }

    @Override
    public <T> T newInstance(String cname, Class<T> expectedType) {
        return delegate.newInstance(cname, expectedType);
    }

    public ResourceLoader getDelegate() {
        return delegate;
    }

    public void setDelegate(ResourceLoader delegate) {
        this.delegate = delegate;
    }

}
