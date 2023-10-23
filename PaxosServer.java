package dslabs.paxos;

import com.google.common.collect.Sets;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import dslabs.shardkv.PaxosDecision;
import dslabs.shardmaster.ShardMaster.Query;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import java.util.Map;
import org.checkerframework.checker.units.qual.A;


@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
    /** All servers in the Paxos group, including this one. */
    private final Address[] servers;
    // not including this one
    private Address[] serversExceptSelf;
    private Ballot ballot;

    private boolean isLeader;

    private AMOApplication<Application> app;

    private Map<Integer, LogEntry> log;

    private Map<Integer, LogEntry> tempLog;
    // The first value that isn't chosen.
    private int slotOut;
    // where to accept a new value.
    private int slotIn;

    private Set<Address>  P1BAcceptors;

    private Map<Integer, Set<Address>>  P2BAcceptors;

    private boolean isLeaderAlive;

    // for garbage collection as leader
    private Map<Address, Integer> otherSlotOuts;

    private int firstNonCleared;

    private Address parentAddress;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosServer(Address address, Address[] servers, Application app) {
        super(address);
        this.servers = servers;
        this.app = new AMOApplication<>(app);

        setup(address);
    }

    public PaxosServer(Address address, Address[] servers, Address parentAddress) {
        super(address);

        this.servers = servers;
        this.parentAddress = parentAddress;

        setup(address);
    }

    private void setup(Address address) {
        this.log = new HashMap<>();
        this.slotIn = 1;
        this.slotOut = 1;
        this.firstNonCleared = 1;
        this.isLeaderAlive = false;
        this.isLeader = servers.length == 1;
        this.ballot = new Ballot(0, address);
        this.P2BAcceptors = new HashMap<>();
        this.P1BAcceptors = new HashSet<>();
        this.otherSlotOuts = new HashMap<>();
        List<Address> serversList = new ArrayList<>(List.of(servers));
        serversList.remove(address);
        this.serversExceptSelf = serversList.toArray(new Address[0]);
    }

    @Override
    public void init() {
        onP1ATimer(new P1ATimer());
    }

    /* -------------------------------------------------------------------------
        Interface Methods

        Be sure to implement the following methods correctly. The test code uses
        them to check correctness more efficiently.
       -----------------------------------------------------------------------*/

    /**
     * Return the status of a given slot in the server's local log.
     *
     * If this server has garbage-collected this slot, it should return {@link
     * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen
     * command for this slot. If this server has both accepted and chosen a
     * command for this slot, it should return {@link PaxosLogSlotStatus#CHOSEN}.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's status
     *
     * @see PaxosLogSlotStatus
     */
    public PaxosLogSlotStatus status(int logSlotNum) {
        if (logSlotNum < firstNonCleared) {
            return PaxosLogSlotStatus.CLEARED;
        }

        if (log.get(logSlotNum) == null) {
            if (slotOut > logSlotNum) {
                return PaxosLogSlotStatus.CLEARED;
            } else {
                return PaxosLogSlotStatus.EMPTY;
            }
        } else {
            return log.get(logSlotNum).status();
        }
    }

    /**
     * Return the command associated with a given slot in the server's local
     * log.
     *
     * If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
     * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}.
     * Otherwise, return the command this server has chosen or accepted,
     * according to {@link PaxosServer#status}.
     *
     * If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this
     * method should unwrap them before returning.
     *
     * Log slots are numbered starting with 1.
     *
     * @param logSlotNum
     *         the index of the log slot
     * @return the slot's contents or {@code null}
     *
     * @see PaxosLogSlotStatus
     */
    public Command command(int logSlotNum) {
        if (status(logSlotNum) ==
                PaxosLogSlotStatus.CLEARED || status(logSlotNum) == PaxosLogSlotStatus.EMPTY) {
            return null;
        }
        if (log.get(logSlotNum).command() == null) {
            return null;
        }
        return log.get(logSlotNum).command().command();
    }

    /**
     * Return the index of the first non-cleared slot in the server's local log.
     * The first non-cleared slot is the first slot which has not yet been
     * garbage-collected. By default, the first non-cleared slot is 1.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int firstNonCleared() {
        return firstNonCleared;
    }

    /**
     * Return the index of the last non-empty slot in the server's local log,
     * according to the defined states in {@link PaxosLogSlotStatus}. If there
     * are no non-empty slots in the log, this method should return 0.
     *
     * Log slots are numbered starting with 1.
     *
     * @return the index in the log
     *
     * @see PaxosLogSlotStatus
     */
    public int lastNonEmpty() {
        return slotIn - 1;
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/

    // messages to handle if leader
    private void handlePaxosRequest(PaxosRequest m, Address sender) {
        // check if query and is leader, lab 4. You are shard master
        if (isLeader && m.command().command() instanceof Query) {
            Result res = app.executeReadOnly(m.command().command());
            // sequence number doesn't matter because we are not doing
            // linearizable reads
            send(new PaxosReply(new AMOResult(res, m.command().sequenceNum(), address())), sender);
            return;
        }
        // check amo app if already executed
        if (app != null && app.alreadyExecuted(m.command())) {
            AMOResult res = app.execute(m.command());
            send(new PaxosReply(res), sender);
            return;
        }
        // if not leader ignore
        if (!isLeader) {
            return;
        }
        // don't accept the same message in another slot
        for (Integer slot : log.keySet()) {
            AMOCommand command = log.get(slot).command();
            if (command == null) continue;

            if (command.clientAddress().equals(m.command().clientAddress())
                    && command.sequenceNum() == m.command().sequenceNum()
                    && command.sequenceNum() != Integer.MAX_VALUE) {
                return;
            }
        }

        // add to log as accepted at slot in
        this.log.put(
                slotIn, new LogEntry(this.ballot, PaxosLogSlotStatus.ACCEPTED, m.command()));

        // send the p2a
        P2A msg = new P2A(this.ballot, this.slotIn, m.command());
        handleP2A(msg, address());
        broadcast(msg, serversExceptSelf);
    }

    private void handleP1B(P1B m, Address sender) {
        // If message ballot number is lower than current ballot number, ignore. Invalid.
        if (m.ballot().compareTo(this.ballot) < 0 || isLeader) {
            return;
        }

        // If message ballot number is same, accepted & P1B is valid
        //  adds that node to list of nodes that accepted request
        this.P1BAcceptors.add(sender);

        // merge received log with our current log
        this.tempLog = mergeLogsTemp(this.tempLog, m.log());

        // When a majority is reached, become the leader and merge the current log with the temp log.
        if(P1BAcceptors.size() > (this.serversExceptSelf.length  +1)/2){
            this.log = mergeLogsFinal(this.tempLog, this.log);
            this.isLeader = true;

            tempLog.clear();
            P1BAcceptors.clear();

            // send heartbeat and p2a
            onHeartbeatTimer(new HeartbeatTimer());
            onP2ATimer(new P2ATimer());
        }
    }

    private void handleP2B(P2B m, Address sender) {
        // ignore if not the leader
        if (!isLeader) {
            return;
        }

        // if ballot is lower than current ignore
        if (m.ballot().compareTo(this.ballot) < 0) {
            return;
        }

        int slot = m.slot();

        // if we already have chosen / cleared ignore
        if (status(slot) == PaxosLogSlotStatus.CHOSEN || status(slot) == PaxosLogSlotStatus.CLEARED) {
            return;
        }

        // If ballot number is the same, add sender (acceptor) to P2B accept receipts at the slot number.
        if (this.P2BAcceptors.get(slot) == null) {
            this.P2BAcceptors.put(slot, new HashSet<>());
        }
        this.P2BAcceptors.get(slot).add(sender);

        // Check if received a majority of accepts. If so, update log at the slot number to be chosen.
        if (this.P2BAcceptors.get(slot).size() > (this.serversExceptSelf.length  +1)/2) {
            AMOCommand chosenValue = this.log.get(slot).command();
            this.log.put(slot, new LogEntry(ballot, PaxosLogSlotStatus.CHOSEN, chosenValue));
            this.P2BAcceptors.remove(slot);
            // try to execute chosen values
            tryExecute();
        }
    }

    private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
        // ignore if not the leader
        if (!isLeader) {
          return;
        }

        //  if it is the first time we get this replica, add to list of slots outs
        if (!otherSlotOuts.containsKey(sender)) {
            otherSlotOuts.put(sender, 0);
        }

        // update slot out of the sender
        if (m.slotOut() > otherSlotOuts.get(sender)) {
            otherSlotOuts.put(sender, m.slotOut());
            // checks if all other servers are past a slot out number,
            // if  so, it can garbage collect all entries before that slot out number in the log
            int lowestSlotOut = slotOut;
            for (Address server : serversExceptSelf) {
                if (!otherSlotOuts.containsKey(server)) {
                    lowestSlotOut = 1;
                } else {
                    lowestSlotOut = Math.min(lowestSlotOut, otherSlotOuts.get(server));
                }
            }

            if (lowestSlotOut > firstNonCleared) {
                garbageCollect(lowestSlotOut);
            }
        }
    }


    // messages to handle if acceptor
    private void handleP1A(P1A m, Address sender) {
        if (checkBallot(m.ballot())) {
            // accept, reply back with p1b
            P1B msg = new P1B(ballot, log);
            if (sender.equals(address())) {
                handleP1B(msg, address());
            } else {
                send(msg, sender);
            }
        }
    }

    private void handleP2A(P2A m, Address sender) {
        if (checkBallot(m.ballot())) {
            // accept, reply back with p2b. Update our log.

            if (status(m.slot()) == PaxosLogSlotStatus.CLEARED || status(m.slot()) == PaxosLogSlotStatus.CHOSEN) {
                return;
            }

            log.put(m.slot(), new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED, m.command()));
            // update slot in here
            slotIn = Math.max(m.slot() + 1, slotIn);

            P2B msg = new P2B(ballot, m.slot());
            if (sender.equals(address())) {
                handleP2B(msg, address());
            } else {
                send(msg, sender);
            }
        }
    }


    // messages to handle if replica
    private void handleHeartbeat(Heartbeat m, Address sender) {
        if (checkBallot(m.ballot())) {
            isLeaderAlive = true;
            if (firstNonCleared <= m.firstNonCleared()) {
                // sync messages, execute chosens, update sloutout
                syncLog(m.log());
                tryExecute();
                // reply with a heartbeat reply with new sloutout
                if (!address().equals(sender)) {
                    send(new HeartbeatReply(slotOut), sender);
                }

                // garbage collect
                if (m.firstNonCleared() > firstNonCleared) {
                    garbageCollect(m.firstNonCleared());
                }
            }
        }
    }


    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private void onHeartbeatTimer(HeartbeatTimer t) {
        // is leader? send the heartbeat
        if (isLeader) {
            Heartbeat msg = new Heartbeat(ballot, log, firstNonCleared);
            broadcast(msg, serversExceptSelf);
            set(t, HeartbeatTimer.HEARTBEAT_MILLIS);
        }
    }

    private void onP1ATimer(P1ATimer t) {
        // is leader dead? isn't leader? increment ballot, run for election.
        if (!isLeader && !isLeaderAlive) {
            // If ballot address isn't the same increase ballot number and reset p1b receipt list
            if (!ballot.server().equals(address()) || ballot.number() == 0) {
                P1BAcceptors.clear();
                ballot = new Ballot(ballot.number()+1, this.address());
                // reset temp log
                tempLog = new HashMap<>();
            }
            // only send p1bs to servers not in your p1b receipt list
            List<Address> toSendTo = new LinkedList<>(Arrays.asList(serversExceptSelf));
            for (Address votedServer : P1BAcceptors) {
                toSendTo.remove(votedServer);
            }
            P1A msg = new P1A(ballot);
            handleP1A(msg, address());
            broadcast(msg, toSendTo.toArray(new Address[0]));
        }
        isLeaderAlive = false;
        set(t, P1ATimer.P1A_MILLIS);
    }

    private void onP2ATimer(P2ATimer t) {
        // is leader? go through log, propose any values that are accepted.
        if (isLeader) {
            for (Integer slot : log.keySet()) {
                if (status(slot) == PaxosLogSlotStatus.ACCEPTED) {
                    List<Address> toSendTo = new LinkedList<>(Arrays.asList(serversExceptSelf));

                    // only send to servers that havent accepted that p2b
                    if (P2BAcceptors.containsKey(slot)) {
                        for (Address acceptedServer : P2BAcceptors.get(slot)) {
                            toSendTo.remove(acceptedServer);
                        }
                    }

                    P2A msg = new P2A(ballot, slot, log.get(slot).command());
                    handleP2A(msg, address());
                    broadcast(msg, toSendTo.toArray(new Address[0]));
                }

            }
            set(t, P2ATimer.P2A_MILLIS);
        }
    }

    /* -------------------------------------------------------------------------
        Utils
       -----------------------------------------------------------------------*/

    private void garbageCollect(int firstNonCleared) {
        this.firstNonCleared = firstNonCleared;
        Set<Integer> slots = new HashSet<>(this.log.keySet());
        for (Integer slot : slots) {
            if (slot < firstNonCleared) {
                // garbage collect it
                this.log.remove(slot);
            }
        }
    }

    /**
     * Checks if the received ballot is the same or higher. If is currently the leader
     * and the other ballot is higher, steps down as the leader, and sets current
     * ballot to the received ballot.
     * @param other the received ballot
     * @return the result.
     */
    private boolean checkBallot(Ballot other) {
        int i = this.ballot.compareTo(other);
        if (i < 0) {
            isLeaderAlive = true;
            isLeader = false;
            otherSlotOuts.clear();
            this.tempLog.clear();
            this.P1BAcceptors.clear();
            this.P2BAcceptors.clear();
            this.ballot = other;
        }
        return i <= 0;
    }

    /**
     * Syncs current log with the leader's log. Changes values to chosen if they
     * are chosen in the leader's log.
     * @param other the leader's log
     */
    private void syncLog(Map<Integer, LogEntry> other) {
        // For every slot:
        //If chosen on leader’s log, overwrite the slot in own log with the slot in leader’s log.
        for (Map.Entry<Integer , LogEntry> entry : other.entrySet()) {
            if (other.get(entry.getKey()).status().equals(PaxosLogSlotStatus.CHOSEN)) {
                this.log.put(entry.getKey(), other.get(entry.getKey()));
                slotIn = Math.max(entry.getKey() + 1, slotIn);
            }
        }
    }

    /**
     * Executes up through the last chosen value on the AMOApplication.
     * Updates slotOut accordingly. Replies to client if leader.
     */
    private void tryExecute() {
        // while slotOut is chosen:
        // execute at slotOut
        // increment slotOut
        while (status(slotOut) == PaxosLogSlotStatus.CHOSEN) {
            if (log.get(slotOut).command() != null) {
                // no op
                if (app != null) {
                    AMOResult res = app.execute(log.get(slotOut).command());
                    if (isLeader) {
                        send(new PaxosReply(res), res.clientAddress());
                    }
                } else {
                    handleMessage(new PaxosDecision(log.get(slotOut).command().command()), parentAddress);
                }
            }
            slotOut++;
        }
    }

    /**
     * Merges the temp log and the final log. Adds no-ops, puts our ballot on
     * every slot.
     * @return The resulting merged log.
     */
    private Map<Integer, LogEntry> mergeLogsFinal(Map<Integer, LogEntry> log1, Map<Integer, LogEntry> log2) {
        Map<Integer, LogEntry> mergedLogs = new HashMap<>();

        Set<Integer> allSlots = Sets.union(log1.keySet(), log2.keySet());

        if (allSlots.size() == 0) {
            return mergedLogs;
        }

        int minSlot = Collections.min(allSlots);
        int maxSlot = Collections.max(allSlots);


        for (int slot = minSlot; slot <= maxSlot; slot++) {
            LogEntry entry1 = log1.get(slot);
            LogEntry entry2 = log2.get(slot);

            if (entry1 == null && entry2 == null) {
                // adding no ops
                mergedLogs.put(slot,
                        new LogEntry(ballot, PaxosLogSlotStatus.ACCEPTED,
                                null));
            } else if (entry1 == null) {
                // if log2 has thing
                mergedLogs.put(slot, new LogEntry(ballot, entry2.status(), entry2.command()));
            } else if (entry2 == null) {
                // if log 1 has thing
                mergedLogs.put(slot, new LogEntry(ballot, entry1.status(), entry1.command()));
            } else {
                // both have thing
                if (entry2.status() == PaxosLogSlotStatus.CHOSEN) {
                    // log 2 chosen
                    mergedLogs.put(slot, new LogEntry(ballot, entry2.status(), entry2.command()));
                } else if (entry1.status() == PaxosLogSlotStatus.CHOSEN) {
                    // log 1 chosen
                    mergedLogs.put(slot, new LogEntry(ballot, entry1.status(), entry1.command()));
                } else if (entry1.ballot().compareTo(entry2.ballot()) > 0) {
                    // log 1 higher ballot
                    mergedLogs.put(slot, new LogEntry(ballot, entry1.status(), entry1.command()));
                } else {
                    // log 2 higher ballot
                    mergedLogs.put(slot, new LogEntry(ballot, entry2.status(), entry2.command()));
                }
            }
        }

        // 🙏🙏🙏🙏🙏

        slotIn = maxSlot + 1;
        return mergedLogs;
    }

    /**
     * Merge an incoming P1B log with the temporary log. For every slot in both
     * logs, chooses the one that is chosen, or if both are accepted, chooses the
     * entry with a higher ballot number.
     * @return The merged log.
     */
    private Map<Integer, LogEntry> mergeLogsTemp(Map<Integer, LogEntry> log1, Map<Integer, LogEntry> log2) {
        Map<Integer, LogEntry> mergedLogs = new HashMap<>();

        for (int slot : Sets.union(log1.keySet(), log2.keySet())) {
            LogEntry entry1 = log1.get(slot);
            LogEntry entry2 = log2.get(slot);

            if (entry1 == null) {
                // log2 has an entry
                mergedLogs.put(slot, entry2);
            } else if (entry2 == null) {
                // log1 has an entry
                mergedLogs.put(slot, entry1);
            } else {
                // both have entries:
                if (entry2.status() == PaxosLogSlotStatus.CHOSEN) {
                    // log 2 chosen
                    mergedLogs.put(slot, entry2);
                } else if (entry1.status() == PaxosLogSlotStatus.CHOSEN) {
                    // log 1 chosen
                    mergedLogs.put(slot, entry1);
                } else if (entry1.ballot().compareTo(entry2.ballot()) > 0) {
                    // log 1 higher ballot
                    mergedLogs.put(slot, entry1);
                } else {
                    // log 2 higher ballot
                    mergedLogs.put(slot, entry2);
                }
            }
        }
        // 🙏🙏🙏🙏🙏
        return mergedLogs;
    }
}