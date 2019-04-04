package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;
import java.util.Set;

/**
 * Represent the configuration of a topic security template
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TopicSecurityTemplateConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicSecurityTemplateConfig implements IConfiguration
{
    /** Default interval for consecutive control messages */
    public static final long DEFAULT_CONTROL_MGS_INTERVAL = 500;

    /** Name of the topic security template */
    @XmlAttribute(name = "name", required = true)
    @Getter private String name;

    /** Interval for consecurity control message sents */
    @XmlElement(name = "control_msg_interval", required = true)
    @Getter private Long controlMsgInterval;

    /** All security id's that are allowed to publish on the topic */
    @XmlElement(name = "pub_sec_id", required = true)
    @Getter private Set<Integer> pubSecIds;

    /** All security id's that are allowed to subscribe to the topic */
    @XmlElement(name = "sub_sec_id", required = true)
    @Getter private Set<Integer> subSecIds;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        if (this.name == null)
        {
            throw new VegaException("Missing parameter name in topic security template config");
        }

        if (this.controlMsgInterval == null)
        {
            this.controlMsgInterval = DEFAULT_CONTROL_MGS_INTERVAL;
        }

        if (this.pubSecIds == null || this.pubSecIds.isEmpty())
        {
            throw new VegaException("Missing parameter list publisher security id");
        }

        if (this.subSecIds == null || this.subSecIds.isEmpty())
        {
            throw new VegaException("Missing parameter list subscriber security id");
        }
    }
}
