package com.bbva.kyof.vega.autodiscovery;

import com.bbva.kyof.vega.autodiscovery.subscriber.IAutodiscPubTopicPatternListener;
import lombok.Getter;
import lombok.ToString;

/**
 * Contents of a onNewPubTopicForPattern / onPubTopicForPatternRemoved to pattern action
 */
@ToString
final class AutodiscSubscribeToPubPatternActionContent
{
    /** Topic pattern to onNewPubTopicForPattern to */
    @Getter private final String pattern;

    /** Listener that will receive events related to the pattern */
    @Getter private final IAutodiscPubTopicPatternListener patternListener;

    /**
     * Create a new onNewPubTopicForPattern to pattern contents
     * @param pattern topic pattern to onNewPubTopicForPattern to
     * @param patternListener listener for events related to the topic pattern
     */
    AutodiscSubscribeToPubPatternActionContent(final String pattern, final IAutodiscPubTopicPatternListener patternListener)
    {
        this.pattern = pattern;
        this.patternListener = patternListener;
    }
}
