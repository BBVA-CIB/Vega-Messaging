package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

/**
 * Represents the configuration for a topic security pattern. The configuration comes on the form of pattern / template pair
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TopicSecurityConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicSecurityConfig implements IConfiguration
{
    /** Name of the pattern the topic security config belongs to */
    @XmlAttribute(name = "pattern", required = true)
    @Getter private String pattern;
    
    /** Name of the template the pattern belong to */
    @XmlAttribute(name = "template", required = true)
    @Getter private String template;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        if (this.pattern == null)
        {
            throw new VegaException("Missing compulsory element pattern in TopicSecurityConfig");
        }

        if (this.template == null)
        {
            throw new VegaException("Missing compulsory element template in TopicSecurityConfig");
        }
    }
}
