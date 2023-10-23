package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Message;
import java.util.Map;
import lombok.Data;

@Data
class P1A implements Message {
    private final Ballot ballot;
}

@Data
class P1B implements Message {
    private final Ballot ballot;
    private final Map<Integer, LogEntry> log;
}

@Data
class P2A implements Message {
    private final Ballot ballot;
    private final int slot;
    private final AMOCommand command;
}

@Data
class P2B implements Message {
    private final Ballot ballot;
    private final int slot;
}

@Data
class Heartbeat implements Message {
    private final Ballot ballot;
    private final Map<Integer, LogEntry> log;
    private final int firstNonCleared;
}

@Data
class HeartbeatReply implements Message {
    private final int slotOut;
}