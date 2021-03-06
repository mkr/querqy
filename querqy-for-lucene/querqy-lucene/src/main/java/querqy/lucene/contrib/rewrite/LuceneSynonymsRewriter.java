/**
 * 
 */
package querqy.lucene.contrib.rewrite;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.lucene.analysis.synonym.SynonymMap;

import querqy.model.AbstractNodeVisitor;
import querqy.model.BooleanQuery;
import querqy.model.DisjunctionMaxQuery;
import querqy.model.ExpandedQuery;
import querqy.model.Node;
import querqy.model.Query;
import querqy.model.Term;
import querqy.rewrite.QueryRewriter;

/**
 * @author rene
 *
 */
public class LuceneSynonymsRewriter extends AbstractNodeVisitor<Node> implements QueryRewriter {

   final SynonymMap synonymMap;

   LinkedList<Sequences> sequencesStack = new LinkedList<>();

   public LuceneSynonymsRewriter(SynonymMap synonymMap) {
      this.synonymMap = synonymMap;
   }

   /*
    * (non-Javadoc)
    * 
    * @see qp.rewrite.QueryRewriter#rewrite(qp.model.Query)
    */
   @Override
   public ExpandedQuery rewrite(ExpandedQuery query) {
      Query userQuery = query.getUserQuery();
      if (userQuery != null) {
         visit(userQuery);
      }
      return query;
   }

   @Override
   public Node visit(Query query) {
      return visit((BooleanQuery) query);
   }

   @Override
   public Node visit(DisjunctionMaxQuery disjunctionMaxQuery) {
      sequencesStack.getLast().nextPosition(disjunctionMaxQuery);
      return super.visit(disjunctionMaxQuery);
   }

   @Override
   public Node visit(Term term) {
      if (!term.isGenerated()) {
         try {
            sequencesStack.getLast().putTerm(term);
         } catch (IOException e) {
            throw new RuntimeException(e);
         }
      }
      return super.visit(term);
   }

   @Override
   public Node visit(BooleanQuery booleanQuery) {

      if ((!sequencesStack.isEmpty()) && booleanQuery.getParent() instanceof BooleanQuery) {
         // left-hand siblings might be DMQs with Terms - terminate sequences
         sequencesStack.getLast().apply();
      }

      // new Sequences object for child DMQ/Term objects
      sequencesStack.add(new Sequences(synonymMap));

      super.visit(booleanQuery);

      sequencesStack.removeLast().apply();

      return null;
   }

}
