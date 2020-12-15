package querqy.solr.contrib;

import org.apache.lucene.analysis.util.ResourceLoader;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrRequestInfo;
import querqy.lucene.contrib.rewrite.wordbreak.Morphology;
import querqy.lucene.contrib.rewrite.wordbreak.WordBreakCompoundRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.solr.FactoryAdapter;

import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

public class WordBreakCompoundRewriterFactory implements FactoryAdapter<RewriterFactory> {

    private static final int DEFAULT_MIN_SUGGESTION_FREQ = 1;
    private static final int DEFAULT_MAX_COMBINE_LENGTH = 30;
    private static final int DEFAULT_MIN_BREAK_LENGTH = 3;
    private static final int DEFAULT_MAX_DECOMPOUND_EXPANSIONS = 3;

    @Override
    public RewriterFactory createFactory(final String id, final NamedList<?> args, ResourceLoader resourceLoader) {

        // the minimum frequency of the term in the index' dictionary field to be considered a valid compound
        // or constituent
        final Integer minSuggestionFreq = getOrDefault(args, "minSuggestionFrequency", DEFAULT_MIN_SUGGESTION_FREQ);

        // the maximum length of a combined term
        final Integer maxCombineLength = getOrDefault(args, "maxCombineWordLength", DEFAULT_MAX_COMBINE_LENGTH);

        // the minimum break term length
        final Integer minBreakLength = getOrDefault(args, "minBreakLength", DEFAULT_MIN_BREAK_LENGTH);

        // the index "dictionary" field to verify compounds / constituents
        final String indexField = (String) args.get("dictionaryField");

        // whether query strings should be turned into lower case before trying to compound/decompound
        final boolean lowerCaseInput = getOrDefault(args, "lowerCaseInput", Boolean.FALSE);

        // terms triggering a reversal of the surrounding compound, e.g. "tasche AUS samt" -> samttasche
        final List<String> reverseCompoundTriggerWords = (List<String>) args.get("reverseCompoundTriggerWords");

        final Integer maxDecompoundExpansions = getOrDefault(args, "decompound.maxExpansions",
                DEFAULT_MAX_DECOMPOUND_EXPANSIONS);

        if (maxDecompoundExpansions < 0) {
            throw new IllegalArgumentException("decompound.maxExpansions >= 0 expected. Found: "
                    + maxDecompoundExpansions);
        }

        final boolean verifyDecompoundCollation = getOrDefault(args, "decompound.verifyCollation", Boolean.FALSE);

        // define whether we should always try to add a reverse compound
        final boolean alwaysAddReverseCompounds = getOrDefault(args, "alwaysAddReverseCompounds", Boolean.FALSE);

        final String morphologyName = (String) args.get("morphology");
        final Morphology morphology = morphologyName == null
                ? Morphology.DEFAULT : Enum.valueOf(Morphology.class, morphologyName.toUpperCase(Locale.ROOT));

        // terms that are "protected", i.e. false positives that should never be split and never be result of a combination
        final List<String> protectedWords = (List<String>) args.get("protectedWords");

        // the indexReader has to be supplied on a per-request basis from a request thread-local
        final Supplier<IndexReader> indexReaderSupplier = () ->
                SolrRequestInfo.getRequestInfo().getReq().getSearcher().getIndexReader();

        return new querqy.lucene.contrib.rewrite.wordbreak.WordBreakCompoundRewriterFactory(id, indexReaderSupplier,
                morphology, indexField, lowerCaseInput, minSuggestionFreq, maxCombineLength, minBreakLength,
                reverseCompoundTriggerWords, alwaysAddReverseCompounds, maxDecompoundExpansions,
                verifyDecompoundCollation, protectedWords);
    }

    @Override
    public Class<?> getCreatedClass() {
        return WordBreakCompoundRewriter.class;
    }

    private static <T> T getOrDefault(final NamedList<?> args, String key, T def) {
        Object valueInParameter = args.get(key);
        return valueInParameter == null ? def : (T) valueInParameter;
    }
}
