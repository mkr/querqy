/**
 * 
 */
package querqy.solr;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ProductFloatFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import org.apache.lucene.util.QueryBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;
import org.apache.solr.search.*;

import org.apache.solr.util.SolrPluginUtils;

import querqy.lucene.LuceneQueryUtil;
import querqy.ComparableCharSequence;
import querqy.lucene.rewrite.*;
import querqy.lucene.rewrite.SearchFieldsAndBoosting.FieldBoostModel;
import querqy.lucene.rewrite.cache.TermQueryCache;
import querqy.model.BoostQuery;
import querqy.model.DisjunctionMaxClause;
import querqy.model.DisjunctionMaxQuery;
import querqy.model.ExpandedQuery;
import querqy.model.QuerqyQuery;
import querqy.model.RawQuery;
import querqy.model.Term;
import querqy.parser.QuerqyParser;
import querqy.rewrite.ContextAwareQueryRewriter;
import querqy.rewrite.RewriteChain;

/**
 * @author rene
 *
 */
public class QuerqyDismaxQParser extends ExtendedDismaxQParser {

    /**
     * generated field boost
     */
    public static final String GFB                      = "gfb";

    /**
     * generated query fields (query generated terms only in these fields)
     */
    public static final String GQF                      = "gqf";

    /**
     * field boost model
     */
    public static final String FBM                      = "fbm";

    /**
     * Query-independent field boost model, reading field boost factors from request params
     */
    public static final String FBM_FIXED                = "fixed";

    /**
     * Query-dependent field boost model ('Probabilistic Retrieval Model for Semi-structured Data')
     */
    public static final String FBM_PRMS                 = "prms";

    /**
     * The default field boost model (= {@link #FBM_FIXED})
     */
    public static final String FBM_DEFAULT              = FBM_FIXED;

    /**
     * boost method - the method to integrate Querqy boost queries with the main query
     */
    public static final String QBOOST_METHOD = "qboost.method";

    /**
     *  Integrate Querqy boost queries with the main query as a re-rank query
     */
    public static final String QBOOST_METHOD_RERANK = "rerank";

    /**
     * The number of docs in the main query result to use for re-ranking when qboost.method=rerank
     */
    public static final String QBOOST_RERANK_NUMDOCS = "qboost.rerank.numDocs";

    /**
     * The default value for {@link #QBOOST_RERANK_NUMDOCS}
     */
    public static final int DEFAULT_RERANK_NUMDOCS      = 500;

    /**
     * Add Querqy boost queries to the main query as optional boolean clauses
     */
    public static final String QBOOST_METHOD_OPT = "opt";

    /**
     * The default boost method (= {@link #QBOOST_METHOD})
     */
    public static final String QBOOST_METHOD_DEFAULT = QBOOST_METHOD_OPT;

    /**
     * Control how the score resulting from the {@link org.apache.lucene.search.similarities.Similarity}
     * implementation is integrated into the score of a Querqy boost query
     */
    public static final String QBOOST_SIMILARITY_SCORE = "qboost.similarityScore";

    /**
     * A possible value of QBOOST_SIMILARITY_SCORE: Do not calculate the similarity score for Querqy boost queries.
     * As a result the boost queries are only scored by query boost and field boost but not by any function of DF or TF.
     * Setting qboost.similarityScore=off yields a small performance gain as TF and DF need not be provided.
     */
    public static final String QBOOST_SIMILARITY_SCORE_OFF = "off";

    /**
     * Tie parameter for combining pf, pf2 and pf3 phrase boostings into a dismax query. Defaults to the value
     * of the {@link org.apache.solr.common.params.DisMaxParams.TIE} parameter
     */
    public static final String QPF_TIE = "qpf.tie";

    /**
     * A possible value of QBOOST_SIMILARITY_SCORE: Just use the similarity as set in Solr when scoring Querqy boost queries.
     */
    public static final String QBOOST_SIMILARITY_SCORE_ON = "on";

    /**
     * A possible value of QBOOST_SIMILARITY_SCORE: "document frequency correction" - use the similarity as set in Solr
     * when scoring Querqy boost queries but fake the document frequency so that all term queries under a given
     * {@link org.apache.lucene.search.DisjunctionMaxQuery} us the same document frequency. This avoids situations
     * in which the rarer of two synonymous terms would get a higher score than the more common term. It also fixes
     * the IDF problem for the same term value occurring in two or more different fields with different frequencies.
     *
     */
    public static final String QBOOST_SIMILARITY_SCORE_DFC = "dfc";

    public static final String QBOOST_SIMILARITY_SCORE_DEFAULT = QBOOST_SIMILARITY_SCORE_DFC;

    /**
     * This parameter controls whether field boosting should be applied to Querqy boost queries.
     */
    public static final String QBOOST_FIELD_BOOST = "qboost.fieldBoost";

    /**
     * A possible value of QBOOST_FIELD_BOOST: Do not apply any field boosting to Querqy boost queries.
     */
    public static final String QBOOST_FIELD_BOOST_OFF = "off";

    /**
     * A possible value of QBOOST_FIELD_BOOST: Use the field boosting as set by parameter {@link #FBM} for Querqy boost
     * queries.
     */
    public static final String QBOOST_FIELD_BOOST_ON = "on";

    public static final String QBOOST_FIELD_BOOST_DEFAULT = QBOOST_FIELD_BOOST_ON;

    public static final float DEFAULT_GQF_VALUE = Float.MIN_VALUE;

    static final String MATCH_ALL = "*:*";

    // the QF parsing code is copied from org.apache.solr.util.SolrPluginUtils
    // and
    // org.apache.solr.search.DisMaxQParser, replacing the default boost factor
    // null with 1f
    private static final Pattern whitespacePattern = Pattern.compile("\\s+");
    private static final Pattern caratPattern = Pattern.compile("\\^");

    final Analyzer queryAnalyzer;
    final RewriteChain rewriteChain;
    final QuerqyParser querqyParser;
    final Map<String, Float> userQueryFields;
    final Map<String, Float> generatedQueryFields;
    final LuceneQueryBuilder builder;
    final DocumentFrequencyCorrection dfc;
    final DocumentFrequencyAndTermContextProvider boostDftcp;
    final SearchFieldsAndBoosting boostSearchFieldsAndBoostings;

    protected List<Query> filterQueries = null;
    protected Map<String, Object> context = null;

    protected PublicExtendedDismaxConfiguration config = null;
    protected List<Query> boostQueries = null;

    protected final boolean useReRankForBoostQueries;
    protected final int reRankNumDocs;
    protected final TermQueryCache termQueryCache;

    protected final float qpfTie;

    protected final boolean debugQuery;

    protected Query userQuery = null;

    public QuerqyDismaxQParser(String qstr, SolrParams localParams, SolrParams params,
         SolrQueryRequest req, RewriteChain rewriteChain, QuerqyParser querqyParser, TermQueryCache termQueryCache)
         throws SyntaxError {

        super(qstr, localParams, params, req);

        this.querqyParser = querqyParser;
        this.termQueryCache = termQueryCache;

        if (config == null) {
            // this is a hack that works around ExtendedDismaxQParser keeping the
            // config member var private
            // and avoids calling createConfiguration() twice
            config = createConfiguration(qstr, localParams, params, req);
        }
        IndexSchema schema = req.getSchema();
        queryAnalyzer = schema.getQueryAnalyzer();
        this.rewriteChain = rewriteChain;
      
        SolrParams solrParams = SolrParams.wrapDefaults(localParams, params);
      
        userQueryFields = parseQueryFields(req.getSchema(), solrParams, DisMaxParams.QF, 1f, true);
        generatedQueryFields = parseQueryFields(req.getSchema(), solrParams, GQF, null, false);
        if (generatedQueryFields.isEmpty()) {
            for (Map.Entry<String, Float> entry: userQueryFields.entrySet()) {
                generatedQueryFields.put(entry.getKey(), entry.getValue() * config.generatedFieldBoostFactor);
            }
        } else {
            for (Map.Entry<String, Float> entry: generatedQueryFields.entrySet()) {
                if (entry.getValue() == null) {
                    String name = entry.getKey();
                    Float nonGeneratedBoostFactor = userQueryFields.get(name);
                    if (nonGeneratedBoostFactor == null) {
                        nonGeneratedBoostFactor = 1f;
                    }
                    entry.setValue(nonGeneratedBoostFactor * config.generatedFieldBoostFactor);
                }
            }
        }
        dfc = new DocumentFrequencyCorrection();

        useReRankForBoostQueries = QBOOST_METHOD_RERANK.equals(solrParams.get(QBOOST_METHOD, QBOOST_METHOD_DEFAULT));
        if (useReRankForBoostQueries) {
            reRankNumDocs = solrParams.getInt(QBOOST_RERANK_NUMDOCS, DEFAULT_RERANK_NUMDOCS);
        } else {
            reRankNumDocs = 0;
        }

        final SearchFieldsAndBoosting searchFieldsAndBoosting =
              new SearchFieldsAndBoosting(getFieldBoostModelFromParam(solrParams), 
                      userQueryFields, generatedQueryFields, config.generatedFieldBoostFactor);
      
        builder = new LuceneQueryBuilder(dfc, queryAnalyzer, searchFieldsAndBoosting, config.getTieBreaker(), termQueryCache);


        final String boostSimScore = solrParams.get(QBOOST_SIMILARITY_SCORE, QBOOST_SIMILARITY_SCORE_DEFAULT);
        if (QBOOST_SIMILARITY_SCORE_DFC.equals(boostSimScore)) {
            boostDftcp = dfc;
        } else if (QBOOST_SIMILARITY_SCORE_ON.equals(boostSimScore)) {
            boostDftcp = new StandardDocumentFrequencyAndTermContextProvider();
        } else if (QBOOST_SIMILARITY_SCORE_OFF.equals(boostSimScore)) {
            boostDftcp = null;
        } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Unknown similarity score handling for Querqy boost queries: " + boostSimScore);
        }

        final String boostFieldBoost = solrParams.get(QBOOST_FIELD_BOOST, QBOOST_FIELD_BOOST_DEFAULT);
        if (boostFieldBoost.equals(QBOOST_FIELD_BOOST_ON)) {
            boostSearchFieldsAndBoostings = searchFieldsAndBoosting;
        } else if (boostFieldBoost.equals(QBOOST_FIELD_BOOST_OFF)) {
            boostSearchFieldsAndBoostings = searchFieldsAndBoosting.withFieldBoostModel(FieldBoostModel.NONE);
        } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                    "Unknown field boost model for Querqy boost queries: " + boostFieldBoost);
        }

        qpfTie = solrParams.getFloat(QPF_TIE, config.getTieBreaker());

        debugQuery = req.getParams().getBool(CommonParams.DEBUG_QUERY, false);

    }
   
   protected FieldBoostModel getFieldBoostModelFromParam(SolrParams solrParams) {
       String fbm = solrParams.get(FBM, FBM_DEFAULT);
       if (fbm.equals(FBM_FIXED)) {
           return FieldBoostModel.FIXED;
       } else if (fbm.equals(FBM_PRMS)) {
           return FieldBoostModel.PRMS;
       }
       
       throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, 
               "Unknown field boost model: " + fbm);
   }

   @Override
   protected PublicExtendedDismaxConfiguration createConfiguration(String qstr,
         SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      // this is a hack that works around ExtendedDismaxQParser keeping the
      // config member var private
      // and avoids calling createConfiguration() twice
      this.config = new PublicExtendedDismaxConfiguration(localParams, params, req);// super.createConfiguration(qstr,
                                                                                    // localParams,
                                                                                    // params,
                                                                                    // req);
      return config;
   }

   /*
    * (non-Javadoc)
    * 
    * @see org.apache.solr.search.QParser#parse()
    */
   @Override
   public Query parse() throws SyntaxError {
      // TODO q.alt
      String userQueryString = getString();
      if (userQueryString == null) {
         throw new SyntaxError("query string is null");
      }
      userQueryString = userQueryString.trim();
      if (userQueryString.length() == 0) {
         throw new SyntaxError("query string is empty");
      }

      Query mainQuery;
      ExpandedQuery expandedQuery = null;
      List<Query> querqyBoostQueries = null;
      final Query phraseFieldQuery;

      if ((userQueryString.charAt(0) == '*') && (userQueryString.length() == 1 || MATCH_ALL.equals(userQueryString))) {
          mainQuery = new MatchAllDocsQuery();
          dfc.finishedUserQuery();
          phraseFieldQuery = null;
      } else {
          expandedQuery = makeExpandedQuery();
          phraseFieldQuery = makePhraseFieldQueries(expandedQuery.getUserQuery());
          context = new HashMap<>();
          if (debugQuery) {
              context.put(ContextAwareQueryRewriter.CONTEXT_KEY_DEBUG_ENABLED, true);
          }
          expandedQuery = rewriteChain.rewrite(expandedQuery, context);
         
          mainQuery = makeUserQuery(expandedQuery);
         
          dfc.finishedUserQuery();
         
          applyFilterQueries(expandedQuery);
          querqyBoostQueries = getQuerqyBoostQueries(expandedQuery);

      }

      boostQueries = getBoostQueries();
      List<Query> boostFunctions = getBoostFunctions();
      List<ValueSource> multiplicativeBoosts = getMultiplicativeBoosts();

      boolean hasMultiplicativeBoosts = multiplicativeBoosts != null && !multiplicativeBoosts.isEmpty();
      boolean hasQuerqyBoostQueries = querqyBoostQueries != null && !querqyBoostQueries.isEmpty();

       // do we have to add a boost query as an optional clause to the main query?
      boolean hasOptBoost = (boostQueries != null && !boostQueries.isEmpty())
            || (boostFunctions != null && !boostFunctions.isEmpty())
            || (phraseFieldQuery != null)
            || hasMultiplicativeBoosts
            || (hasQuerqyBoostQueries && !useReRankForBoostQueries);

      if (hasOptBoost) {

          BooleanQuery.Builder builder = new BooleanQuery.Builder();

          builder.add(mainQuery, Occur.MUST);

         if (boostQueries != null) {
            for (Query f : boostQueries) {
                builder.add(f, BooleanClause.Occur.SHOULD);
            }
         }

         if (boostFunctions != null) {
            for (Query f : boostFunctions) {
                builder.add(f, BooleanClause.Occur.SHOULD);
            }
         }

         if (phraseFieldQuery != null) {
             builder.add(phraseFieldQuery, BooleanClause.Occur.SHOULD);
         }

         if (hasQuerqyBoostQueries && !useReRankForBoostQueries) {
            for (Query q : querqyBoostQueries) {
                builder.add(q, BooleanClause.Occur.SHOULD);
            }
         }

         BooleanQuery bq = builder.build();

         if (hasMultiplicativeBoosts) {
            if (multiplicativeBoosts.size() > 1) {
               ValueSource prod = new ProductFloatFunction(
                     multiplicativeBoosts.toArray(new ValueSource[multiplicativeBoosts.size()]));
               mainQuery = new BoostedQuery(bq, prod);
            } else {
               mainQuery = new BoostedQuery(bq, multiplicativeBoosts.get(0));
            }
         } else {
            mainQuery = bq;
         }
      }

      if (useReRankForBoostQueries && hasQuerqyBoostQueries) {

          final BooleanQuery.Builder builder = new BooleanQuery.Builder();

          for (final Query q : querqyBoostQueries) {
              builder.add(q, BooleanClause.Occur.SHOULD);
          }

          mainQuery = new QuerqyReRankQuery(mainQuery, builder.build(), reRankNumDocs, 1.0);
      }

      return mainQuery;

   }

    @Override
    public Query getHighlightQuery() throws SyntaxError {
        if (userQuery == null) {
            parse();
        }
        return userQuery;
    }

    protected List<Query> getQuerqyBoostQueries(final ExpandedQuery expandedQuery) throws SyntaxError {

      List<Query> result = transformBoostQueries(expandedQuery.getBoostUpQueries(), 1f);
      List<Query> down = transformBoostQueries(expandedQuery.getBoostDownQueries(), -1f);

      if (down != null) {
         if (result == null) {
            result = down;
         } else {
            result.addAll(down);
         }
      }

      return result;
   }

    public List<Query> transformBoostQueries(final Collection<BoostQuery> boostQueries, final float factor)
            throws SyntaxError {

        final List<Query> result;

        if (boostQueries != null && !boostQueries.isEmpty()) {

            result = new LinkedList<>();

            for (BoostQuery bq : boostQueries) {

                Query luceneQuery = null;
                QuerqyQuery<?> boostQuery = bq.getQuery();

                if (boostQuery instanceof RawQuery) {

                    QParser bqp = QParser.getParser(((RawQuery) boostQuery).getQueryString(), null, req);
                    luceneQuery = bqp.getQuery();

                } else if (boostQuery instanceof querqy.model.Query) {

                    LuceneQueryBuilder luceneQueryBuilder =
                            new LuceneQueryBuilder(boostDftcp, queryAnalyzer,
                                    boostSearchFieldsAndBoostings,
                                    config.getTieBreaker(), termQueryCache);
                    try {

                        luceneQuery = luceneQueryBuilder.createQuery((querqy.model.Query) boostQuery, factor < 0f);

                        if (luceneQuery != null) {
                            luceneQuery = wrapQuery(luceneQuery);
                        }

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                }

                if (luceneQuery != null) {
                    final float boost = bq.getBoost() * factor;
                    if (boost != 1f) {
                        result.add(new org.apache.lucene.search.BoostQuery(luceneQuery, boost));
                    } else {
                        result.add(luceneQuery);

                    }

                }

            }

        } else {
            result = null;
        }

        return result;
    }

    public ExpandedQuery makeExpandedQuery() {

        return new ExpandedQuery(querqyParser.parse(qstr));

    }


    public Query makePhraseFieldQueries(final querqy.model.Query userQuery) {


        if (userQuery != null) {

            final List<querqy.model.BooleanClause> clauses = userQuery.getClauses();

            if (clauses.size() > 1) {

                final List<FieldParams> allPhraseFields = new LinkedList<>();
                final IndexSchema schema = req.getSchema();

                for (final FieldParams field : config.getAllPhraseFields()) {
                    if (isFieldPhraseQueryable(schema.getFieldOrNull(field.getField()))) {
                        allPhraseFields.add(field);
                    }
                }



                if (allPhraseFields != null && !allPhraseFields.isEmpty()) {

                    final List<String> sequence = new LinkedList<>();

                    for (final querqy.model.BooleanClause clause : clauses) {

                        if (clause instanceof DisjunctionMaxQuery) {

                            final DisjunctionMaxQuery dmq = (DisjunctionMaxQuery) clause;

                            if (dmq.occur != querqy.model.SubQuery.Occur.MUST_NOT) {

                                for (final DisjunctionMaxClause dmqClause : dmq.getClauses()) {
                                    if (dmqClause instanceof Term) {

                                        final ComparableCharSequence value = ((Term) dmqClause).getValue();
                                        final int length = value.length();
                                        final StringBuilder sb = new StringBuilder(length);
                                        for (int i = 0; i < length; i++) {
                                            sb.append(value.charAt(i));
                                        }
                                        sequence.add(sb.toString());
                                        break;
                                    }
                                }

                            }
                        }

                    }

                    if (sequence.size() > 1) {

                        final List<Query> disjuncts = new LinkedList<>();

                        final QueryBuilder queryBuilder = new QueryBuilder(schema.getQueryAnalyzer());

                        final List<String>[] shingles = new List[4];
                        String pf = null;


                        for (final FieldParams fieldParams : allPhraseFields) {

                            final int n = fieldParams.getWordGrams();
                            final int slop = fieldParams.getSlop();
                            final String fieldname = fieldParams.getField();

                            if (n == 0) {

                                if (pf == null) {
                                    final StringBuilder sb = new StringBuilder(getString().length());
                                    for (final String term : sequence) {
                                        if (sb.length() > 0) {
                                            sb.append(' ');
                                        }
                                        sb.append(term);
                                    }
                                    pf = sb.toString();
                                }
                                final Query pq = queryBuilder.createPhraseQuery(fieldname, pf, slop);
                                if (pq != null) {
                                    disjuncts.add(LuceneQueryUtil.boost(pq, fieldParams.getBoost()));
                                }

                            } else if (n <= sequence.size()) {

                                if (shingles[n] == null) {
                                    shingles[n] = new LinkedList<>();
                                    for (int i = 0, lenI = sequence.size() - n + 1; i < lenI; i++) {
                                        final StringBuilder sb = new StringBuilder();

                                        for (int j = i, lenJ = j + n; j < lenJ; j++) {
                                            if (sb.length() > 0) {
                                                sb.append(' ');
                                            }
                                            sb.append(sequence.get(j));
                                        }
                                        shingles[n].add(sb.toString());
                                    }
                                }


                                final List<Query> nGramQueries = new ArrayList<>(shingles[n].size());

                                for (final String nGram : shingles[n]) {
                                    final Query pq = queryBuilder.createPhraseQuery(fieldname, nGram, slop);
                                    if (pq != null) {
                                        nGramQueries.add(pq);
                                    }
                                }

                                switch (nGramQueries.size()) {
                                    case 0: break;
                                    case 1: {

                                        final Query nGramQuery = nGramQueries.get(0);
                                        disjuncts.add(LuceneQueryUtil.boost(nGramQuery, fieldParams.getBoost()));
                                        break;

                                    }
                                    default:

                                        final BooleanQuery.Builder builder = new BooleanQuery.Builder();

                                        for (final Query nGramQuery : nGramQueries) {
                                            builder.add(nGramQuery, Occur.SHOULD);
                                        }

                                        final BooleanQuery bq = builder.build();
                                        disjuncts.add(LuceneQueryUtil.boost(bq, fieldParams.getBoost()));
                                }
                            }
                        }

                        switch (disjuncts.size()) {
                            case 0: break;
                            case 1: return disjuncts.get(0);
                            default :
                                return new org.apache.lucene.search.DisjunctionMaxQuery(disjuncts, qpfTie);
                        }

                    }


                }
            }
        }

        return null;

    }

    public boolean isFieldPhraseQueryable(final SchemaField field) {
        if (field != null) {
            final FieldType fieldType = field.getType();
            if ((fieldType instanceof TextField) && !field.omitPositions() && !field.omitTermFreqAndPositions()) {
                return true;
            }
        }

        return false;
    }

   /**
    * @param query
    */
   public Query applyMinShouldMatch(final Query query) {

       if (!(query instanceof BooleanQuery)) {
           return query;
       }

       final BooleanQuery bq = (BooleanQuery) query;
       List<BooleanClause> clauses = bq.clauses();
       if (clauses.size() < 2) {
           return bq;
       }

       for (BooleanClause clause : clauses) {
           if ((clause.getQuery() instanceof BooleanQuery) && (clause.getOccur() != Occur.MUST)) {
               return bq; // seems to be a complex query with sub queries - do not
               // apply mm
           }
       }

       return SolrPluginUtils.setMinShouldMatch(bq, config.getMinShouldMatch());

   }


    public void applyFilterQueries(final ExpandedQuery expandedQuery) throws SyntaxError {

      Collection<QuerqyQuery<?>> filterQueries = expandedQuery.getFilterQueries();

      if (filterQueries != null && !filterQueries.isEmpty()) {
          
          List<Query> fqs = new LinkedList<>();
          
          for (QuerqyQuery<?> qfq : filterQueries) {
          
              if (qfq instanceof RawQuery) {
                  
                  QParser fqParser = QParser.getParser(((RawQuery) qfq).getQueryString(), null, req);
                  fqs.add(fqParser.getQuery());
                  
              } else if (qfq instanceof querqy.model.Query) {
                  
                  builder.reset();
                  
                  try {
                      fqs.add(builder.createQuery((querqy.model.Query) qfq));
                  } catch (IOException e) {
                        throw new RuntimeException(e);
                  }
                  
              }
          }

          this.filterQueries = fqs;
      }
         
   }

   public Query makeUserQuery(final ExpandedQuery expandedQuery) {

      builder.reset();

      try {

          userQuery = wrapQuery(
                  applyMinShouldMatch(
                          builder.createQuery(expandedQuery.getUserQuery())));

          return userQuery;

      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

    public Query wrapQuery(final Query query) {

        if (query == null || dfc == null) {

            return query;

        } else {

            final WrappedQuery wrappedQuery = new WrappedQuery(query);
            wrappedQuery.setCache(true);
            wrappedQuery.setCacheSep(false);
            return wrappedQuery;

        }

    }
   
   public List<Query> getFilterQueries() {
       return filterQueries;
   }

   public Map<String, Object> getContext() {
       return context;
   }

   /**
    * Copied from DisMaxQParser
    * 
    * @param solrParams
    * @return
    * @throws SyntaxError
    */
   public Query getAlternateUserQuery(final SolrParams solrParams) throws SyntaxError {
      String altQ = solrParams.get(DisMaxParams.ALTQ);
      if (altQ != null) {
         QParser altQParser = subQuery(altQ, null);
         return altQParser.getQuery();
      } else {
         return null;
      }
   }

   /**
    * Given a string containing fieldNames and boost info, converts it to a Map
    * from field name to boost info.
    *
    * <p>
    * Doesn't care if boost info is negative, you're on your own.
    * </p>
    * <p>
    * Doesn't care if boost info is missing, again: you're on your own.
    * </p>
    *
    * @param in
    *           a String like "fieldOne^2.3 fieldTwo fieldThree^-0.4"
    * @return Map of fieldOne =&gt; 2.3, fieldTwo =&gt; null, fieldThree =&gt;
    *         -0.4
    */
   public static Map<String, Float> parseFieldBoosts(final String in, final Float defaultBoost) {
      return parseFieldBoosts(new String[] { in }, defaultBoost);
   }

   /**
    * Like <code>parseFieldBoosts(String)</code>, but parses all the strings in
    * the provided array (which may be null).
    *
    * @param fieldLists
    *           an array of Strings eg.
    *           <code>{"fieldOne^2.3", "fieldTwo", fieldThree^-0.4}</code>
    * @return Map of fieldOne =&gt; 2.3, fieldTwo =&gt; null, fieldThree =&gt;
    *         -0.4
    */
   public static Map<String, Float> parseFieldBoosts(final String[] fieldLists, final Float defaultBoost) {
      if (null == fieldLists || 0 == fieldLists.length) {
         return new HashMap<>();
      }
      final Map<String, Float> out = new HashMap<>(7);
      for (String in : fieldLists) {
         if (null == in) {
            continue;
         }
         in = in.trim();
         if (in.length() == 0) {
            continue;
         }

         final String[] bb = whitespacePattern.split(in);
         for (final String s : bb) {
            final String[] bbb = caratPattern.split(s);
            out.put(bbb[0], 1 == bbb.length ? defaultBoost : Float.valueOf(bbb[1]));
         }
      }
      return out;
   }

   /**
    * Copied from DisMxQParser (as we don't handle user fields/aliases yet)
    * 
    * Uses {@link SolrPluginUtils#parseFieldBoosts(String)} with the 'qf'
    * parameter. Falls back to the 'df' parameter.
    */
   public static Map<String, Float> parseQueryFields(final IndexSchema indexSchema, final SolrParams solrParams,
                                                     final String fieldName, final Float defaultBoost, final boolean useDfFallback)
         throws SyntaxError {
      final Map<String, Float> queryFields = parseFieldBoosts(solrParams.getParams(fieldName), defaultBoost);
      if (queryFields.isEmpty() && useDfFallback) {
         final String df = solrParams.get(CommonParams.DF);
         if (df == null) {
            throw new SyntaxError("Neither " + fieldName + ", " + CommonParams.DF
                  + ", nor the default search field are present.");
         }
         queryFields.put(df, defaultBoost);
      }
      return queryFields;
   }

   public class PublicExtendedDismaxConfiguration extends ExtendedDismaxConfiguration {

      final float generatedFieldBoostFactor;

      /**
       * @param localParams
       * @param params
       * @param req
       */
      public PublicExtendedDismaxConfiguration(final SolrParams localParams,
            final SolrParams params, final SolrQueryRequest req) {
         super(localParams, params, req);
         generatedFieldBoostFactor = solrParams.getFloat(GFB, 1f);
      }

      public float getTieBreaker() {
         return tiebreaker;
      }

      public String getMinShouldMatch() {
         return minShouldMatch;
      }

   }


}
