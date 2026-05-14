package org.eventviewer.leader;

public interface LeaderElectionService {

    boolean isLeader();

    long getFencingToken();
}
