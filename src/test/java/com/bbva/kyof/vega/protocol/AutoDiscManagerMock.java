package com.bbva.kyof.vega.protocol;

import com.bbva.kyof.vega.autodiscovery.AutodiscManager;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicInfo;
import com.bbva.kyof.vega.autodiscovery.model.AutoDiscTopicSocketInfo;
import lombok.Getter;
import org.easymock.EasyMock;

import java.util.HashSet;
import java.util.Set;

import static org.easymock.EasyMock.getCurrentArguments;

/**
 * Created by cnebrera on 12/08/16.
 */
public class AutoDiscManagerMock
{
    @Getter final AutodiscManager mock;

    @Getter final Set<AutoDiscTopicInfo> regTopicInfos = new HashSet<>();
    @Getter final Set<AutoDiscTopicSocketInfo> regTopicSocketInfos = new HashSet<>();

    public AutoDiscManagerMock()
    {
        // Mock auto-discovery manager calls
        mock = EasyMock.createNiceMock(AutodiscManager.class);
        mock.registerTopicInfo(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> this.topicInfoRegistered((AutoDiscTopicInfo) getCurrentArguments()[0])).anyTimes();
        mock.unregisterTopicInfo(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> this.topicInfoUnregistered((AutoDiscTopicInfo) getCurrentArguments()[0])).anyTimes();
        mock.registerTopicSocketInfo(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> this.topicSocketInfoRegistered((AutoDiscTopicSocketInfo) getCurrentArguments()[0])).anyTimes();
        mock.unregisterTopicSocketInfo(EasyMock.anyObject());
        EasyMock.expectLastCall().andAnswer(() -> this.topicSocketInfoUnregistered((AutoDiscTopicSocketInfo) getCurrentArguments()[0])).anyTimes();
        EasyMock.replay(mock);
    }

    private Object topicInfoRegistered(final AutoDiscTopicInfo topicInfo)
    {
        regTopicInfos.add(topicInfo);
        return null;
    }

    private Object topicInfoUnregistered(final AutoDiscTopicInfo topicInfo)
    {
        regTopicInfos.remove(topicInfo);
        return null;
    }

    private Object topicSocketInfoRegistered(final AutoDiscTopicSocketInfo topicInfo)
    {
        regTopicSocketInfos.add(topicInfo);
        return null;
    }

    private Object topicSocketInfoUnregistered(final AutoDiscTopicSocketInfo topicInfo)
    {
        regTopicSocketInfos.remove(topicInfo);
        return null;
    }
}
