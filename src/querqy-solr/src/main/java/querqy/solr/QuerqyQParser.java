/**
 * 
 */
package querqy.solr;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.util.SolrPluginUtils;

import querqy.antlr.ANTLRQueryParser;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.lucene.LuceneQueryBuilder;

/**
 * @author rene
 *
 */
public class QuerqyQParser extends QParser {
    
    static final String MATCH_ALL = "*:*";
    
    // the QF parsing code is copied from org.apache.solr.util.SolrPluginUtils and 
    // org.apache.solr.search.DisMaxQParser, replacing the default boost factor null with 1f
    private static final Pattern whitespacePattern = Pattern.compile("\\s+");
    private static final Pattern caratPattern = Pattern.compile("\\^");
    
    final Analyzer queryAnalyzer;
    final RewriteChain rewriteChain;

    public QuerqyQParser(String qstr, SolrParams localParams, SolrParams params,
            SolrQueryRequest req, RewriteChain rewriteChain) {
        super(qstr, localParams, params, req);
        IndexSchema schema = req.getSchema();
        queryAnalyzer = schema.getQueryAnalyzer();
        this.rewriteChain = rewriteChain;
    }

    /* (non-Javadoc)
     * @see org.apache.solr.search.QParser#parse()
     */
    @Override
    public Query parse() throws SyntaxError {
        
        // TODO q.alt
        String userQuery = getString();
        if (userQuery == null) {
            throw new SyntaxError("query string is null");
        }
        userQuery = userQuery.trim();
        if (userQuery.length() == 0) {
            throw new SyntaxError("query string is empty");
        }
        
        if ((userQuery.charAt(0) == '*') && (userQuery.length() == 1 || MATCH_ALL.equals(userQuery))) {
            return new MatchAllDocsQuery();
        }
        
        SolrParams solrParams = SolrParams.wrapDefaults(localParams, params);
        Map<String, Float> queryFields = parseQueryFields(req.getSchema(), solrParams);
        
        LuceneQueryBuilder builder = new LuceneQueryBuilder(queryAnalyzer, queryFields);
        
        ANTLRQueryParser parser = new ANTLRQueryParser();
        querqy.model.Query q = parser.parse(qstr);
        
        return builder.createQuery(rewriteChain.rewrite(q, Collections.<String,Object>emptyMap()));
        
    }
    
    /**
     * Copied from DisMaxQParser
     * @param solrParams
     * @return
     * @throws SyntaxError
     */
    public Query getAlternateUserQuery(SolrParams solrParams) throws SyntaxError {
        String altQ = solrParams.get(DisMaxParams.ALTQ);
        if (altQ != null) {
          QParser altQParser = subQuery(altQ, null);
          return altQParser.getQuery();
        } else {
          return null;
        }
      }
    
    /**
     * Given a string containing fieldNames and boost info,
     * converts it to a Map from field name to boost info.
     *
     * <p>
     * Doesn't care if boost info is negative, you're on your own.
     * </p>
     * <p>
     * Doesn't care if boost info is missing, again: you're on your own.
     * </p>
     *
     * @param in a String like "fieldOne^2.3 fieldTwo fieldThree^-0.4"
     * @return Map of fieldOne =&gt; 2.3, fieldTwo =&gt; null, fieldThree =&gt; -0.4
     */
    public static Map<String,Float> parseFieldBoosts(String in) {
      return parseFieldBoosts(new String[]{in});
    }
    /**
     * Like <code>parseFieldBoosts(String)</code>, but parses all the strings
     * in the provided array (which may be null).
     *
     * @param fieldLists an array of Strings eg. <code>{"fieldOne^2.3", "fieldTwo", fieldThree^-0.4}</code>
     * @return Map of fieldOne =&gt; 2.3, fieldTwo =&gt; null, fieldThree =&gt; -0.4
     */
    public static Map<String,Float> parseFieldBoosts(String[] fieldLists) {
      if (null == fieldLists || 0 == fieldLists.length) {
        return new HashMap<>();
      }
      Map<String, Float> out = new HashMap<>(7);
      for (String in : fieldLists) {
        if (null == in) {
          continue;
        }
        in = in.trim();
        if(in.length()==0) {
          continue;
        }
        
        String[] bb = whitespacePattern.split(in);
        for (String s : bb) {
          String[] bbb = caratPattern.split(s);
          out.put(bbb[0], 1 == bbb.length ? 1f : Float.valueOf(bbb[1]));
        }
      }
      return out;
    }
    
    
    /**
     * Uses {@link SolrPluginUtils#parseFieldBoosts(String)} with the 'qf' parameter. Falls back to the 'df' parameter
     * or {@link org.apache.solr.schema.IndexSchema#getDefaultSearchFieldName()}.
     */
    public static Map<String, Float> parseQueryFields(final IndexSchema indexSchema, final SolrParams solrParams)
        throws SyntaxError {
      Map<String, Float> queryFields = parseFieldBoosts(solrParams.getParams(DisMaxParams.QF));
      if (queryFields.isEmpty()) {
        String df = QueryParsing.getDefaultField(indexSchema, solrParams.get(CommonParams.DF));
        if (df == null) {
          throw new SyntaxError("Neither "+DisMaxParams.QF+", "+CommonParams.DF +", nor the default search field are present.");
        }
        queryFields.put(df, 1.0f);
      }
      return queryFields;
    }

}