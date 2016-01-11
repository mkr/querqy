/**
 * 
 */
package querqy.lucene.rewrite;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.search.DisjunctionMaxQuery;


/**
 * @author rene
 *
 */
public class DisjunctionMaxQueryFactory implements LuceneQueryFactory<DisjunctionMaxQuery> {

    protected final LinkedList<LuceneQueryFactory<?>> disjuncts;
   
    public DisjunctionMaxQueryFactory() {
       disjuncts = new LinkedList<>();
   }

    public void add(LuceneQueryFactory<?> disjunct) {
       disjuncts.add(disjunct);
   }

    public int getNumberOfDisjuncts() {
       return disjuncts.size();
   }

    public LuceneQueryFactory<?> getFirstDisjunct() {
       return disjuncts.getFirst();
   }

    @Override
    public void prepareDocumentFrequencyCorrection(DocumentFrequencyCorrection dfc, boolean isBelowDMQ) {
        if ((!isBelowDMQ) && (dfc != null)) {
            dfc.newClause();
        }
        for (LuceneQueryFactory<?> disjunct : disjuncts) {
            disjunct.prepareDocumentFrequencyCorrection(dfc, true);
        }
    }

    @Override
    public DisjunctionMaxQuery createQuery(FieldBoost boost, float dmqTieBreakerMultiplier, DocumentFrequencyCorrection dfc, boolean isBelowDMQ) throws IOException {
       

        DisjunctionMaxQuery dmq = new DisjunctionMaxQuery(dmqTieBreakerMultiplier);
      

        for (LuceneQueryFactory<?> disjunct : disjuncts) {
            dmq.add(disjunct.createQuery(boost, dmqTieBreakerMultiplier, dfc, true));
        }

        return dmq;
    }


}
