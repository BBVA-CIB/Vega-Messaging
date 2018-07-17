package com.bbva.kyof.vega.protocol.common;

import com.bbva.kyof.vega.autodiscovery.IAutodiscManager;
import com.bbva.kyof.vega.config.general.GlobalConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import io.aeron.Aeron;
import lombok.Getter;
import lombok.Setter;

import java.util.Timer;
import java.util.UUID;

/**
 * Stores the manager instance context with common information that is going to go through
 */
public class VegaContext
{

    /** Aeron context */
    @Getter private final Aeron aeron;

    /** Instance unique identifier */
    @Getter private final UUID instanceUniqueId = UUID.randomUUID();

    /** Timer that controls the tasks related with heartbeats */
    @Getter private final Timer heartbeatsTimer = new Timer("VegaHeartbeatTimer_" + instanceUniqueId);

    /** The configuration of the manager instance */
    @Getter private final GlobalConfiguration instanceConfig;

    /** Autodiscovery manager*/
    @Getter @Setter private IAutodiscManager autodiscoveryManager;

    /** Asynchronous request manager */
    @Getter @Setter private AsyncRequestManager asyncRequestManager;

    /** Security context */
    @Getter private VegaSecurityContext securityContext = null;

    /**
     * Construct a new vega context instance
     * @param aeron the Aeron instance
     * @param instanceConfig the instance configuration
     */
    public VegaContext(final Aeron aeron, final GlobalConfiguration instanceConfig)
    {
        this.aeron = aeron;
        this.instanceConfig = instanceConfig;
    }

    /**
     * Stop and clean the internal timer for heartbeats
     */
    public void stopHeartsbeatTimer()
    {
        this.heartbeatsTimer.cancel();
        this.heartbeatsTimer.purge();
    }

    /**
     *  Initialize the security information using the provided security params. It will ignore the call if there are no parameters.
     * @param securityParams the security parameters
     * @throws VegaException exception thrown if there is a problem
     */
    public void initializeSecurity(final SecurityParams securityParams) throws VegaException
    {
        this.securityContext = new VegaSecurityContext(securityParams, this.instanceConfig.getAllSecureTopicsSecurityIds());
    }
}