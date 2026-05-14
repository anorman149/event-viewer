package org.eventviewer.leader;

@FunctionalInterface
public interface LeaderTask {

    void execute() throws Exception;
}
