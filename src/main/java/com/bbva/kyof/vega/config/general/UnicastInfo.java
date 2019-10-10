package com.bbva.kyof.vega.config.general;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Contains the address and port for one resolver daemon
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "UnicastInfo")
@NoArgsConstructor
@AllArgsConstructor
public class UnicastInfo
{
    /** (Optional, only unicast) The resolver daemon address */
    @XmlElement(name = "resolver_daemon_address")
    @Getter private String resolverDaemonAddress;

    /** (Optional, only unicast) The resolver daemon port */
    @XmlElement(name = "resolver_daemon_port")
    @Getter @Setter private Integer resolverDaemonPort;
}
