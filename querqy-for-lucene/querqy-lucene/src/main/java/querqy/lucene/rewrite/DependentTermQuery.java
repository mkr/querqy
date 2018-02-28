/**
 * 
 */
package querqy.lucene.rewrite;

import java.io.IOException;
import java.util.Set;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * A TermQuery that depends on other term queries for the calculation of the document frequency
 * and/or the boost factor (field weight). 
 * 
 * @author René Kriegler, @renekrie
 *
 */
public class DependentTermQuery extends TermQuery {
    
    final int tqIndex;
    final DocumentFrequencyAndTermContextProvider dftcp;
    final FieldBoost fieldBoost;

    public DependentTermQuery(Term term, DocumentFrequencyAndTermContextProvider dftcp, FieldBoost fieldBoost) {
        this(term, dftcp, dftcp.termIndex(), fieldBoost);
    }

    protected DependentTermQuery(Term term, DocumentFrequencyAndTermContextProvider dftcp, int tqIndex, FieldBoost fieldBoost) {

        super(term);

        if (fieldBoost == null) {
            throw new IllegalArgumentException("FieldBoost must not be null");
        }

        if (dftcp == null) {
            throw new IllegalArgumentException("DocumentFrequencyAndTermContextProvider must not be null");
        }

        if (term == null) {
            throw new IllegalArgumentException("Term must not be null");
        }

        this.tqIndex  = tqIndex;
        this.dftcp = dftcp;
        this.fieldBoost = fieldBoost;
    }
    
    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        DocumentFrequencyAndTermContextProvider.DocumentFrequencyAndTermContext dftc = dftcp.getDocumentFrequencyAndTermContext(tqIndex, searcher);
        if (dftc.df < 1) {
            return new NeverMatchWeight();
        }
        return new TermWeight(searcher, needsScores, boost, dftc.termContext);
        
    }
    

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime  + tqIndex;
        result = prime * result + fieldBoost.hashCode();
       // result = prime * result + getTerm().hashCode(); handled in super class
        return super.hashCode() ^ result;
    }

    @Override
    public boolean equals(Object obj) {

        if (!super.equals(obj)) {
            return false;
        }

        DependentTermQuery other = (DependentTermQuery) obj;
        if (tqIndex != other.tqIndex)
            return false;
        if (!fieldBoost.equals(other.fieldBoost))
            return false;

        return true; // getTerm().equals(other.getTerm());  already assured in super class

    }
    
    @Override
    public String toString(String field) {
        Term term = getTerm();
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
          buffer.append(term.field());
          buffer.append(":");
        }
        buffer.append(term.text());
        buffer.append(fieldBoost.toString(term.field()));
        return buffer.toString();
        
    }
    
    public FieldBoost getFieldBoost() {
        return fieldBoost;
    }
    
    /**
     * Copied from inner class in {@link TermQuery}
     *
     */
    final class TermWeight extends Weight {
        private final Similarity similarity;
        private final Similarity.SimWeight stats;
        private final TermContext termStates;
        private final boolean needsScores;

        public TermWeight(IndexSearcher searcher, boolean needsScores, float boost, TermContext termStates)
          throws IOException {
            super(DependentTermQuery.this);
            if (needsScores && termStates == null) {
                throw new IllegalStateException("termStates are required when scores are needed");
            }
            Term term = getTerm();
            this.needsScores = needsScores;
            this.termStates = termStates;
            this.similarity = searcher.getSimilarity(needsScores);

            final CollectionStatistics collectionStats;
            final TermStatistics termStats;
            if (needsScores) {
                collectionStats = searcher.collectionStatistics(term.field());
                termStats = searcher.termStatistics(term, termStates);
            } else {
                // we do not need the actual stats, use fake stats with docFreq=maxDoc=ttf=1
                collectionStats = new CollectionStatistics(term.field(), 1, 1, 1, 1);
                termStats = new TermStatistics(term.bytes(), 1, 1);
            }

            if (termStats == null) {
                this.stats = null; // term doesn't exist in any segment, we won't use similarity at all
            } else {
                float fieldBoostFactor = fieldBoost.getBoost(getTerm().field(), searcher.getIndexReader());
                this.stats = similarity.computeWeight(fieldBoostFactor * boost, collectionStats, termStats);
            }
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            terms.add(getTerm());
        }

        @Override
        public String toString() { return "weight(" + DependentTermQuery.this + ")"; }


        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {

            assert termStates != null && termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context))
                    : "The top-reader used to create Weight is not the same as the current reader's top-reader: " + ReaderUtil.getTopLevelContext(context);
            final TermsEnum termsEnum = getTermsEnum(context);
            if (termsEnum == null) {
              return null;
            }
            IndexOptions indexOptions = context.reader()
                    .getFieldInfos()
                    .fieldInfo(getTerm().field())
                    .getIndexOptions();
            PostingsEnum docs = termsEnum.postings(null, needsScores ? PostingsEnum.FREQS : PostingsEnum.NONE);
            assert docs != null;
            return new TermScorer(this, docs, similarity.simScorer(stats, context));
        }

        private long getMaxFreq(IndexOptions indexOptions, long ttf, long df) {
            // TODO: store the max term freq?
            if (indexOptions.compareTo(IndexOptions.DOCS) <= 0) {
                // omitTFAP field, tf values are implicitly 1.
                return 1;
            } else {
                assert ttf >= 0;
                return Math.min(Integer.MAX_VALUE, ttf - df + 1);
            }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
        
        /**
         * Returns a {@link TermsEnum} positioned at this weights Term or null if
         * the term does not exist in the given context
         */
        private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
          Term term = getTerm();
            if (termStates != null) {
              // TermQuery either used as a Query or the term states have been provided at construction time
              assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) : "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
              final TermState state = termStates.get(context.ord);
              if (state == null) { // term is not present in that reader
                assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
                return null;
              }
              final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
              termsEnum.seekExact(term.bytes(), state);
              return termsEnum;
            } else {
              // TermQuery used as a filter, so the term states have not been built up front
              Terms terms = context.reader().terms(term.field());
              if (terms == null) {
                return null;
              }
              final TermsEnum termsEnum = terms.iterator();
              if (termsEnum.seekExact(term.bytes())) {
                return termsEnum;
              } else {
              return null;
            }
          }
        }
        
        private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
          // only called from assert
          //System.out.println("TQ.termNotInReader reader=" + reader + " term=" + field + ":" + bytes.utf8ToString());
          return reader.docFreq(term) == 0;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            TermScorer scorer = (TermScorer) scorer(context);
            if (scorer != null) {
              int newDoc = scorer.iterator().advance(doc);
              if (newDoc == doc) {
                float freq = scorer.freq();
                SimScorer docScorer = similarity.simScorer(stats, context);
                Explanation freqExplanation = Explanation.match(freq, "freq, occurrences of term within document");
                Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
                return Explanation.match(
                    scoreExplanation.getValue(),
                    "weight(" + getQuery() + " in " + doc + ") ["
                        + similarity.getClass().getSimpleName() + ", " + fieldBoost.getClass().getSimpleName() + "], result of:",
                    scoreExplanation );
              }
            }
            return Explanation.noMatch("no matching term");
        }

    }

    public class NeverMatchWeight extends Weight {

        protected NeverMatchWeight() {
            super(DependentTermQuery.this);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc)
                throws IOException {
            return Explanation.noMatch("no matching term");
        }

        @Override
        public Scorer scorer(LeafReaderContext context)
                throws IOException {
            return null;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            terms.add(getTerm());
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }
    }

    
}
