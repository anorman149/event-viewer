package org.eventviewer.leader;

public interface LeaderListener {
    void onElected();

    default void onRevoked() {
        //no-op
    }
}
