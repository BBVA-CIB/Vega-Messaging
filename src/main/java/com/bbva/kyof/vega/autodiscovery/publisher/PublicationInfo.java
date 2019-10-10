package com.bbva.kyof.vega.autodiscovery.publisher;

import io.aeron.Publication;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

/**
 * Class that contains the information about the state of an unicast daemon
 */
@AllArgsConstructor
@Getter
public class PublicationInfo
{
    /** Publication aeron socket used to send the messages */
    private final Publication publication;

    /** Unique id of the vega library instance of this client */
    @Setter private UUID uniqueId;

    /** Unicast resolver client ip where it received the resolver daemon messages  */
    @Setter private int unicastResolverServerIp;

    /** Unicast resolver client port where it received the resolver daemon messages */
    @Setter private int unicastResolverServerPort;

    /** Indicates if the unicast daemon of this publication is active or not*/
    @Setter private Boolean enabled;
}
