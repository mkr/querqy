package querqy.solr.io;

import org.apache.lucene.analysis.util.ResourceLoader;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.util.zip.GZIPInputStream;

/**
 * A {@link org.apache.lucene.analysis.util.ResourceLoader} which detects GZIP compression by its magic byte signature
 * and decompresses if necessary.
 */
public class GzipAwareResourceLoader extends DelegatingResourceLoader {

    @Override
    public InputStream openResource(String resource) throws IOException {
        InputStream is = getDelegate().openResource(resource);
        PushbackInputStream pb = new PushbackInputStream(is, 2);
        if (isInputStreamGZIPCompressed(pb)) {
            return new GZIPInputStream(pb);
        } else {
            return pb;
        }
    }

    private static boolean isInputStreamGZIPCompressed(final PushbackInputStream is) throws IOException {
        if (is == null) {
            return false;
        }

        byte[] signature = new byte[2];
        int count = 0;
        try {
            while (count < 2) {
                int readCount = is.read(signature, count, 2 - count);
                if (readCount < 0) return false;
                count = count + readCount;
            }
        } finally {
            is.unread(signature, 0, count);
        }
        int streamHeader = ((int) signature[0] & 0xff) | ((signature[1] << 8) & 0xff00);
        return GZIPInputStream.GZIP_MAGIC == streamHeader;
    }
}
