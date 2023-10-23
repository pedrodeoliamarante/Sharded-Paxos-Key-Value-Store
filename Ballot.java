package dslabs.paxos;

import dslabs.framework.Address;
import java.io.Serializable;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class Ballot implements Comparable<Ballot>, Serializable {
    private final int number;
    private final Address server;

    public int compareTo(Ballot other) {
        if (number == other.number) {
            return server.compareTo(other.server);
        }
        return number - other.number;
    }
}
