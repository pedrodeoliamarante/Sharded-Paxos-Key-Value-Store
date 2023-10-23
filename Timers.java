package dslabs.paxos;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
    static final int CLIENT_RETRY_MILLIS = 100;
    private final PaxosRequest request;
}

@Data
final class P1ATimer implements Timer {
    static final int P1A_MILLIS = 100;
}

@Data
final class P2ATimer implements Timer {
    static final int P2A_MILLIS = 50;
}

@Data
final class HeartbeatTimer implements Timer {
    static final int HEARTBEAT_MILLIS = 25;
}