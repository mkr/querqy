/**
 *
 */
package querqy.solr;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.search.QParser;

import org.apache.solr.search.RankQuery;
import querqy.infologging.InfoLoggingContext;
import querqy.rewrite.SearchEngineRequestAdapter;
import querqy.rewrite.commonrules.model.DecorateInstruction;

/**
 * @author René Kriegler, @renekrie
 *
 */
public class QuerqyQueryComponent extends QueryComponent {

    /* (non-Javadoc)
     * @see org.apache.solr.handler.component.SearchComponent#prepare(org.apache.solr.handler.component.ResponseBuilder)
     */
    @Override
    public void prepare(final ResponseBuilder rb) throws IOException {

        super.prepare(rb);

        QParser parser = rb.getQparser();

        if (parser instanceof QuerqyDismaxQParser) {

            List<Query> filterQueries = ((QuerqyDismaxQParser) parser).getFilterQueries();
            if ((filterQueries != null) && !filterQueries.isEmpty()) {
                List<Query> filters = rb.getFilters();
                if (filters == null) {
                    rb.setFilters(filterQueries);
                } else {
                    filters.addAll(filterQueries);
                }
            }

            // add the RankQuery to the ResponseBuilder, only if it does not already contain one (set by Solr's rq parameter)
            Optional<RankQuery> maybeRankQuery = ((QuerqyDismaxQParser) parser).getRankQuery();
            if (rb.getRankQuery() == null) {
                maybeRankQuery.ifPresent(rb::setRankQuery);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.solr.handler.component.SearchComponent#process(org.apache.solr.handler.component.ResponseBuilder)
     */
    @Override
    public void process(final ResponseBuilder rb) throws IOException {

        super.process(rb);

        final QParser parser = rb.getQparser();

        if (parser instanceof QuerqyDismaxQParser) {

            final SearchEngineRequestAdapter searchEngineRequestAdapter =
                    ((QuerqyDismaxQParser) parser).getSearchEngineRequestAdapter();

            final Map<String, Object> context = searchEngineRequestAdapter.getContext();
            if (context != null) {

                @SuppressWarnings("unchecked")
                final Set<Object> decorations = (Set<Object>) context.get(DecorateInstruction.DECORATION_CONTEXT_KEY);
                if (decorations != null) {
                    rb.rsp.add("querqy_decorations", decorations);
                }

                @SuppressWarnings("unchecked")
                final Map<String, Object> namedDecorations =
                        (Map<String, Object>) context.get(DecorateInstruction.DECORATION_CONTEXT_MAP_KEY);

                if (namedDecorations != null) {
                    rb.rsp.add("querqy_named_decorations", namedDecorations);
                }

            }

            searchEngineRequestAdapter.getInfoLoggingContext().ifPresent(InfoLoggingContext::endOfRequest);
        }

    }

    /* (non-Javadoc)
     * @see org.apache.solr.handler.component.SearchComponent#getDescription()
     */
    @Override
    public String getDescription() {
        return "Querqy search component";
    }

}
