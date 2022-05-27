
package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.util.net.SubnetAddress;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Parameters for an control subscriber */
@ToString
@AllArgsConstructor
@EqualsAndHashCode
public class ControlSubscriberParams
{
    /** Ip used for the publication (0 for ipc)*/
    @Getter private final int ipAddress;

    /** Port used for the publication (0 for ipc)*/
    @Getter private final int port;

    /** StreamId used by the publication */
    @Getter private final int streamId;

    /** Subnet for the publication (null for ipc) */
    @Getter private final SubnetAddress subnetAddress;
}  