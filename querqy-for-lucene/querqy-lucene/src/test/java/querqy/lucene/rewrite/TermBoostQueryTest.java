package querqy.lucene.rewrite;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Created by rene on 11/09/2016.
 */
public class TermBoostQueryTest extends LuceneTestCase {

    @Test
    public void testThatWeightGetsScoreFromFieldBoost() throws Exception {

        final float fieldBoostFactor = 2f;

        ConstantFieldBoost fieldBoost = new ConstantFieldBoost(fieldBoostFactor);

        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, analyzer);

        indexWriter.close();


        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader);

        final TermBoostQuery tbq = new TermBoostQuery(new Term("f1", "v1"), fieldBoost);

        final Weight weight = tbq.createWeight(indexSearcher, true, 1f);

        assertTrue(weight instanceof TermBoostQuery.TermBoostWeight);
        final TermBoostQuery.TermBoostWeight tbw = (TermBoostQuery.TermBoostWeight) weight;

        assertEquals(fieldBoostFactor, tbw.getUnnormalizedScore(), 0.0001f);

        indexReader.close();
        directory.close();
        analyzer.close();

    }

    @Test
    public void testExtractTerms() throws Exception {

        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, analyzer);

        TestUtil.addNumDocs("f1", "v1", indexWriter, 1);

        indexWriter.close();


        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader);

        final Set<Term> terms = new HashSet<>();
        final Term term = new Term("f1", "v1");
        new TermBoostQuery(term, new ConstantFieldBoost(1f))
                .createWeight(indexSearcher, true, 1f)
                .extractTerms(terms);

        assertTrue(terms.contains(term));

        indexReader.close();
        directory.close();
        analyzer.close();

    }

    @Test
    public void testThatSimilarityIsNotUsed() throws Exception {

        ConstantFieldBoost fieldBoost = new ConstantFieldBoost(1f);

        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, analyzer);

        TestUtil.addNumDocs("f1", "v1", indexWriter, 1);

        indexWriter.close();


        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader);

        Similarity similarity = mock(Similarity.class);
        indexSearcher.setSimilarity(similarity);
        TermBoostQuery termBoostQuery = new TermBoostQuery(new Term("f1", "v1"), fieldBoost);
        indexSearcher.search(termBoostQuery, 10);

        verify(similarity, never()).computeWeight(
                Matchers.anyFloat(),
                Matchers.any(CollectionStatistics.class),
                Matchers.<TermStatistics>anyVararg()
                );



        indexReader.close();
        directory.close();
        analyzer.close();

    }

    @Test
    public void testThatResultsAreStillFound() throws Exception {
        ConstantFieldBoost fieldBoost = new ConstantFieldBoost(1f);

        Analyzer analyzer = new KeywordAnalyzer();

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, analyzer);

        TestUtil.addNumDocs("f1", "v1", indexWriter, 1);
        TestUtil.addNumDocs("f1", "v2", indexWriter, 1);

        indexWriter.close();


        IndexReader indexReader = DirectoryReader.open(directory);
        IndexSearcher indexSearcher = newSearcher(indexReader);

        TermBoostQuery termBoostQuery = new TermBoostQuery(new Term("f1", "v1"), fieldBoost);
        TopDocs topDocs = indexSearcher.search(termBoostQuery, 10);

        assertEquals(1, topDocs.totalHits);
        Document resultDoc = indexSearcher.doc(topDocs.scoreDocs[0].doc);
        assertEquals("v1", resultDoc.get("f1"));

        indexReader.close();
        directory.close();
        analyzer.close();

    }
}
