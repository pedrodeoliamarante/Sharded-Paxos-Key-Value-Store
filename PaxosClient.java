package dslabs.paxos;

import com.google.common.base.Objects;
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
    private final Address[] servers;
    private PaxosRequest request;
    private PaxosReply reply;
    private int sequenceNum;

    /* -------------------------------------------------------------------------
        Construction and Initialization
       -----------------------------------------------------------------------*/
    public PaxosClient(Address address, Address[] servers) {
        super(address);
        this.servers = servers;
    }

    @Override
    public synchronized void init() {
        // No need to initialize
    }

    /* -------------------------------------------------------------------------
        Public methods
       -----------------------------------------------------------------------*/
    @Override
    public synchronized void sendCommand(Command operation) {
//        if (request != null && reply == null) {
//            return;
//        }

        AMOCommand amoCommand = new AMOCommand(operation, sequenceNum++, address());
        request = new PaxosRequest(amoCommand);
        reply = null;

        broadcast(request, servers) ;
        set(new ClientTimer(request), ClientTimer.CLIENT_RETRY_MILLIS);
    }

    @Override
    public synchronized boolean hasResult() {
      
        return reply != null;
    }

    @Override
    public synchronized Result getResult() throws InterruptedException {

        while (reply == null) {
            wait();
        }
        return reply.result().result();
    }

    /* -------------------------------------------------------------------------
        Message Handlers
       -----------------------------------------------------------------------*/
    private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
  
        if (m.result().sequenceNum() == request.command().sequenceNum()) {
            reply = m;
            notify();
        }
    }

    /* -------------------------------------------------------------------------
        Timer Handlers
       -----------------------------------------------------------------------*/
    private synchronized void onClientTimer(ClientTimer t) {
        if (Objects.equal(request, t.request()) && reply == null) {
            broadcast(request, servers);
            set(new ClientTimer(request), ClientTimer.CLIENT_RETRY_MILLIS);
        }
    }
}
