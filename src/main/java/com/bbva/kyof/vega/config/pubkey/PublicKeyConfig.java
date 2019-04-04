package com.bbva.kyof.vega.config.pubkey;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.xml.bind.annotation.*;

/**
 * This class represents a configuration for a public RSA key
 */
@XmlRootElement(name = "public_key")
@XmlAccessorType(XmlAccessType.FIELD)
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PublicKeyConfig implements IConfiguration
{
    /** The value of the binary key in Base64 */
    @XmlValue
    @Getter
    private String value;

    /** The application security id the key belongs too */
    @XmlAttribute(name = "app_security_id", required = true)
    @Getter private int appSecurityId;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        if (this.value == null)
        {
            throw new VegaException("The key value cannot be null");
        }
    }
}