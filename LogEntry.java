package dslabs.paxos;

import dslabs.atmostonce.AMOCommand;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class LogEntry implements Serializable {
    private final Ballot ballot;
    private final PaxosLogSlotStatus status;
    private final AMOCommand command;
}
