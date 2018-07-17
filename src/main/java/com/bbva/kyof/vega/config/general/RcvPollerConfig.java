package com.bbva.kyof.vega.config.general;

import com.bbva.kyof.vega.config.IConfiguration;
import com.bbva.kyof.vega.exception.VegaException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import javax.xml.bind.annotation.*;

/**
 * Configuration for a receiver poller
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "RcvPollerConfig")
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RcvPollerConfig implements IConfiguration
{
    /** Default value for maximum number of fragments per poll */
    static final int DEFAULT_MAX_FRAGMENTS_POLL = 1;

    /** Name of receive poller */
    @XmlAttribute(name = "name", required = true)
    @Getter private String name;

    /** Idle strategy type to use between consecutive polls*/
    @XmlElement(name = "idle_strategy_type", required = true)
    private IdleStrategyType idleStrategyType;

    /** (Optional) Max fragments per poll. Each fragment by default is around 4k.
     * Since by default we are using automatic assembly there is no need to modify this value for big messages */
    @XmlElement(name = "max_fragments_per_poll")
    @Getter private Integer maxFragmentsPerPoll;
    
    /** (Optional) Idle strategy sleep time, only for sleep strategy */
    @XmlElement(name = "idleStrategy_sleep_nanos")
    private Long idleStrategySleepTime;

    /** Created IdleStrategy for the poller */
    @XmlTransient
    @Getter private IdleStrategy idleStrategy;

    @Override
    public void completeAndValidateConfig() throws VegaException
    {
        if (this.name == null)
        {
            throw new VegaException("Missing parameter name on RcvPollerConfig");
        }

        if (idleStrategyType == null)
        {
            throw new VegaException("Missing parameter idle_strategy_type on RcvPollerConfig");
        }

        // If the strategy is sleep, the sleep time nanos should be provided
        switch (idleStrategyType)
        {
            case BUSY_SPIN:
                this.idleStrategy = new BusySpinIdleStrategy();
                break;
            case BACK_OFF:
                this.idleStrategy = new BackoffIdleStrategy(1, 1, 1, 1);
                break;
            case SLEEP_NANOS:
                if (idleStrategySleepTime == null)
                {
                    throw new VegaException("Sleep time should be provided when sleep nanos idle strategy is used");
                }
                this.idleStrategy = new SleepingIdleStrategy(idleStrategySleepTime);
                break;
            default:
                throw new VegaException("Invalid idle strategy value found");
        }

        if (maxFragmentsPerPoll == null)
        {
            maxFragmentsPerPoll = DEFAULT_MAX_FRAGMENTS_POLL;
        }
    }
}
