package com.ds.replicationlog.statemachine;

import java.util.Optional;

public interface PeerClient<V, R>{
    Optional<R> sendNotification(V message);
}
