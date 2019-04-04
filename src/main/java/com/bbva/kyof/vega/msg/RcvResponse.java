package com.bbva.kyof.vega.msg;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

/**
 * Represent a received response message
 *
 * This class is not thread safe!
 */
public class RcvResponse extends BaseRcvMessage implements IRcvResponse
{
    /** Original request id that has triggered the response */
    @Getter @Setter private UUID originalRequestId;

    @Override
    public IRcvResponse promote()
    {
        RcvResponse promotedResponse = new RcvResponse();
        super.promote(promotedResponse);

        promotedResponse.originalRequestId = this.originalRequestId;

        return promotedResponse;
    }
}
