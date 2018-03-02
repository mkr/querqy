package querqy.lucene.rewrite;

import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

/**
 * Created by rene on 10/09/2016.
 */
public class StandardDocumentFrequencyAndTermContextProvider extends AbstractDocumentFrequencyAndTermContextProvider {

    @Override
    protected TermStats doCalculateTermContexts(IndexSearcher searcher) throws IOException {

        IndexReaderContext topReaderContext = searcher.getTopReaderContext();

        int[] dfs = new int[terms.size()];
        TermContext[] contexts = new TermContext[dfs.length];

        for (int i = 0; i < dfs.length; i++) {

            Term term = terms.get(i);

            contexts[i] = new TermContext(topReaderContext);

            for (final LeafReaderContext ctx : topReaderContext.leaves()) {
                final Terms terms = ctx.reader().terms(term.field());
                if (terms != null) {
                    final TermsEnum termsEnum = terms.iterator();
                    if (termsEnum.seekExact(term.bytes())) {
                        final TermState termState = termsEnum.termState();
                        int df = termsEnum.docFreq();
                        dfs[i] = dfs[i] + df;
                        contexts[i].register(termState, ctx.ord, df, -1);
                    }
                }
            }
        }

        return new TermStats(dfs, contexts, topReaderContext);
    }
}
