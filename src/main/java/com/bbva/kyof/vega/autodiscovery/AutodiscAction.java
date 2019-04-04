package com.bbva.kyof.vega.autodiscovery;

import lombok.Getter;
import lombok.ToString;

/**
 * Represent an action to perform over the autodiscovery (register / unregister / onNewPubTopicForPattern / onPubTopicForPatternRemoved)
 */
@ToString
final class AutodiscAction<T>
{
    /** Stores the action type */
    @Getter private final AutodiscActionType actionType;

    /** The information content to register or unregister */
    @Getter private final T content;

    /**
     * Create a new base action with the given action type
     * @param actionType the action type
     * @param content the content of the action
     */
    AutodiscAction(final AutodiscActionType actionType, final T content)
    {
        this.actionType = actionType;
        this.content = content;
    }
}
