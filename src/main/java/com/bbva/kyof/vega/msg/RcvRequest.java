package com.bbva.kyof.vega.msg;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.agrona.DirectBuffer;

import java.util.UUID;

/**
 * This class represents received request
 *
 * This class is not thread safe!
 */
@NoArgsConstructor
public class RcvRequest extends RcvMessage implements IRcvRequest
{
    /** The unique request id */
    @Getter @Setter private UUID requestId;

    /** The responder object for this request */
    @Setter private IRequestResponder requestResponder;

    @Override
    public IRcvRequest promote()
    {
        final RcvRequest promotedRequest = new RcvRequest();
        super.promote(promotedRequest);

        promotedRequest.requestId = this.requestId;
        promotedRequest.requestResponder = this.requestResponder;

        return promotedRequest;
    }

    @Override
    public PublishResult sendResponse(final DirectBuffer responseContent, final int offset, final int length)
    {
        return this.requestResponder.sendResponse(this.requestId, responseContent, offset, length);
    }
}
