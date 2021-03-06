/**
 * 
 */
package querqy.rewrite;

import java.util.Map;

import querqy.model.ExpandedQuery;

/**
 * If a {@link QueryRewriter} implements this interface, the {@link #rewrite(ExpandedQuery, Map)} method is called 
 * instead of {@link #rewrite(ExpandedQuery)} in the query rewriting chain. The Map argument can be freely used to 
 * pass data between rewriters and to the consumer of the rewrite chain.
 * 
 * @author René Kriegler, @renekrie
 *
 */
public interface ContextAwareQueryRewriter extends QueryRewriter {
    
    ExpandedQuery rewrite(ExpandedQuery query, Map<String, Object> context);

}
