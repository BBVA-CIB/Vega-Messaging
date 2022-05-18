
package com.bbva.kyof.vega.protocol.control;

import com.bbva.kyof.vega.util.net.SubnetAddress;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Parameters for a Control Publisher. The transport type is not included because it is always "unicast" */
@ToString
@AllArgsConstructor
@EqualsAndHashCode
class ControlPublisherParams
{
    /** Ip used for the publication */
    @Getter private final int ipAddress;

    /** Port used for the publication )*/
    @Getter private final int port;

    /** StreamId used by the publication */
    @Getter private final int streamId;

    /** Subnet for the publication  */
    @Getter private final SubnetAddress subnetAddress;

    /** Subnet for the publication  */
    @Getter private final String hostname;
}