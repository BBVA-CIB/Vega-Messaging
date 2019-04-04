package com.bbva.kyof.vega.autodiscovery.model;

import com.bbva.kyof.vega.serialization.IUnsafeSerializable;

import java.util.UUID;

/**
 * Interface implemented by all the auto-discovery information messages that goes through the network. <p>
 *
 * Any message should be serializable and contain a unique id for fast lookup. What is represented by the id depends on the
 * auto-discovery information type.
 */
public interface IAutoDiscInfo extends IUnsafeSerializable
{

    /** @return the unique identifier of the auto-discovery information represented by this object */
    UUID getUniqueId();
}
