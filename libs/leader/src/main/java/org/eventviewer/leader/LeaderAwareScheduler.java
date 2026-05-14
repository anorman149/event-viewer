package org.eventviewer.leader;

public interface LeaderAwareScheduler {

    void runIfLeader(LeaderTask task) throws Exception;
}
