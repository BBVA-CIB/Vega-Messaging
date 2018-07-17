package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;

/**
 * Represents the configuration for a topic pattern. The configuration comes on the form of pattern / template pair
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "TopicConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TopicConfig implements IConfiguration
{
    /** Name of the pattern the topic belongs to */
    @XmlAttribute(name = "pattern", required = true)
    @Getter private String pattern;
    
    /** Name of the template the pattern belong to */
    @XmlAttribute(name = "template", required = true)
    @Getter private String template;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        if (pattern == null)
        {
            throw new VegaException("Missing compulsory element pattern in TopicConfig");
        }

        if (template == null)
        {
            throw new VegaException("Missing compulsory element template in TopicConfig");
        }
    }
}
