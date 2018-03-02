package querqy.solr;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.component.MergeStrategy;
import org.apache.solr.search.QueryCommand;
import org.apache.solr.search.RankQuery;

import java.io.IOException;
import java.util.Set;


/**
 * Created by rene on 01/09/2016.
 */
public class QuerqyReRankQuery extends RankQuery {

    protected static final Query DEFAULT_MAIN_QUERY = new MatchAllDocsQuery();

    protected Query mainQuery = DEFAULT_MAIN_QUERY;

    protected final Query reRankQuery;
    protected final int reRankNumDocs;
    protected final double reRankWeight;

    public QuerqyReRankQuery(final Query mainQuery, final Query reRankQuery, final int reRankNumDocs, final double reRankWeight) {
        super();
        this.reRankQuery = reRankQuery;
        this.reRankNumDocs = reRankNumDocs;
        this.reRankWeight = reRankWeight;
        wrap(mainQuery);
    }

    @Override
    public TopDocsCollector getTopDocsCollector(int len, QueryCommand cmd, IndexSearcher searcher) throws IOException {
        return new ReRankCollector(reRankNumDocs, len, reRankQuery, reRankWeight, cmd, searcher);
    }

    @Override
    public RankQuery wrap(Query mainQuery) {
        if (mainQuery == null) {
            throw new IllegalArgumentException("Cannot wrap null");
        }
        this.mainQuery = mainQuery;
        return this;
    }

    @Override
    public MergeStrategy getMergeStrategy() {
        return null;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query m = mainQuery.rewrite(reader);
        Query r = reRankQuery.rewrite(reader);

        if (m != mainQuery || r != reRankQuery) {
            return new QuerqyReRankQuery(m, r, reRankNumDocs, reRankWeight);
        }
        return super.rewrite(reader);
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        return new ReRankWeight(mainQuery, reRankQuery, reRankWeight, searcher, needsScores, boost);
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (!sameClassAs(o)) return false;

        QuerqyReRankQuery that = (QuerqyReRankQuery) o;

        if (reRankNumDocs != that.reRankNumDocs) return false;

        if (Double.compare(that.reRankWeight, reRankWeight) != 0) return false;

        if (!mainQuery.equals(that.mainQuery)) return false;

        return reRankQuery.equals(that.reRankQuery);

    }

    @Override
    public int hashCode() {
        int prime = 31;
        int result = classHash();
        result = prime * result + mainQuery.hashCode();
        result = prime * result + reRankQuery.hashCode();
        result = prime * result + reRankNumDocs;
        final long temp = Double.doubleToLongBits(reRankWeight);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    private class ReRankWeight extends Weight{
        private Query reRankQuery;
        private IndexSearcher searcher;
        private Weight mainWeight;
        private Weight rankWeight;
        private double reRankWeight;

        public ReRankWeight(Query mainQuery, Query reRankQuery, double reRankWeight, IndexSearcher searcher, boolean needsScores,
                            float boost) throws IOException {
            super(mainQuery);
            this.reRankQuery = reRankQuery;
            this.searcher = searcher;
            this.reRankWeight = reRankWeight;
            this.mainWeight = mainQuery.createWeight(searcher, needsScores, boost);
            this.rankWeight = reRankQuery.createWeight(searcher, true, boost);
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            this.mainWeight.extractTerms(terms);
            this.rankWeight.extractTerms(terms);
        }

        public Scorer scorer(LeafReaderContext context) throws IOException {
            return mainWeight.scorer(context);
        }

        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            Explanation mainExplain = mainWeight.explain(context, doc);
            return new QueryRescorer(reRankQuery) {
                @Override
                protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                    float score = firstPassScore;
                    if (secondPassMatches) {
                        score += reRankWeight * secondPassScore;
                    }
                    return score;
                }
            }.explain(searcher, mainExplain, context.docBase+doc);
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }

    public class ReRankCollector extends TopDocsCollector {

        private Query reRankQuery;
        private TopDocsCollector mainCollector;
        private IndexSearcher searcher;
        private int reRankNumDocs;
        private int length;
        private double reRankWeight;


        public ReRankCollector(int reRankNumDocs,
                               int length,
                               Query reRankQuery,
                               double reRankWeight,
                               QueryCommand cmd,
                               IndexSearcher searcher) throws IOException {

            super(null);

            this.reRankQuery = reRankQuery;
            this.reRankNumDocs = reRankNumDocs;
            this.length = length;
            Sort sort = cmd.getSort();
            if (sort == null) {
                this.mainCollector = TopScoreDocCollector.create(Math.max(reRankNumDocs, length));
            } else {
                sort = sort.rewrite(searcher);
                this.mainCollector = TopFieldCollector.create(sort, Math.max(reRankNumDocs, length), false, true, true);
            }
            this.searcher = searcher;
            this.reRankWeight = reRankWeight;

        }

        public int getTotalHits() {
            return mainCollector.getTotalHits();
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            return mainCollector.getLeafCollector(context);
        }

        @Override
        public boolean needsScores() {
            return true;
        }

        public TopDocs topDocs(int start, int howMany) {

            try {

                TopDocs mainDocs = mainCollector.topDocs(0, Math.max(reRankNumDocs, length));

                if (mainDocs.totalHits == 0 || mainDocs.scoreDocs.length == 0) {
                    return mainDocs;
                }


                ScoreDoc[] mainScoreDocs = mainDocs.scoreDocs;

          /*
          *  Create the array for the reRankScoreDocs.
          */
                ScoreDoc[] reRankScoreDocs = new ScoreDoc[Math.min(mainScoreDocs.length, reRankNumDocs)];

          /*
          *  Copy the initial results into the reRankScoreDocs array.
          */
                System.arraycopy(mainScoreDocs, 0, reRankScoreDocs, 0, reRankScoreDocs.length);

                mainDocs.scoreDocs = reRankScoreDocs;

                TopDocs rescoredDocs = new QueryRescorer(reRankQuery) {
                    @Override
                    protected float combine(float firstPassScore, boolean secondPassMatches, float secondPassScore) {
                        float score = firstPassScore;
                        if (secondPassMatches) {
                            score += reRankWeight * secondPassScore;
                        }
                        return score;
                    }
                }.rescore(searcher, mainDocs, mainDocs.scoreDocs.length);

                //Lower howMany to return if we've collected fewer documents.
                howMany = Math.min(howMany, mainScoreDocs.length);

                if (howMany == rescoredDocs.scoreDocs.length) {
                    return rescoredDocs; // Just return the rescoredDocs
                } else if (howMany > rescoredDocs.scoreDocs.length) {

                    //We need to return more then we've reRanked, so create the combined page.
                    ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
                    //lay down the initial docs
                    System.arraycopy(mainScoreDocs, 0, scoreDocs, 0, scoreDocs.length);
                    //overlay the rescoreds docs
                    System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, rescoredDocs.scoreDocs.length);
                    rescoredDocs.scoreDocs = scoreDocs;
                    return rescoredDocs;
                } else {
                    //We've rescored more then we need to return.
                    ScoreDoc[] scoreDocs = new ScoreDoc[howMany];
                    System.arraycopy(rescoredDocs.scoreDocs, 0, scoreDocs, 0, howMany);
                    rescoredDocs.scoreDocs = scoreDocs;
                    return rescoredDocs;
                }
            } catch (Exception e) {
                throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e);
            }
        }

    }

}
