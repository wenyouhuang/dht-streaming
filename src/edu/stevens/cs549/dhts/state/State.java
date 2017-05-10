package edu.stevens.cs549.dhts.state;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.media.sse.EventOutput;
import org.glassfish.jersey.media.sse.EventSource;
import org.glassfish.jersey.media.sse.OutboundEvent;
import org.glassfish.jersey.media.sse.SseBroadcaster;

import edu.stevens.cs549.dhts.activity.DHTBase;
import edu.stevens.cs549.dhts.activity.IDHTNode;
import edu.stevens.cs549.dhts.activity.NodeInfo;
import edu.stevens.cs549.dhts.resource.TableRep;
import edu.stevens.cs549.dhts.state.State.JoinState;

/**
 * 
 * @author dduggan
 */
public class State implements IState, IRouting {

	static final long serialVersionUID = 0L;

	public static Logger log = Logger.getLogger(State.class.getCanonicalName());
	
	protected NodeInfo info;
	private EventSource BLANK_ENVENT_SOURCE;

	public State(NodeInfo info) {
		super();
		this.info = info;
		this.predecessor = null;
		this.successor = info;

		this.finger = new NodeInfo[NKEYS];
		for (int i = 0; i < NKEYS; i++) {
			finger[i] = info;
		}

	}

	/*
	 * Get the info for this DHT node.
	 */
	public NodeInfo getNodeInfo() {
		return info;
	}

	/*
	 * Local table operations.
	 */
	private Persist.Table dict = Persist.newTable();

	@SuppressWarnings("unused")
	private Persist.Table backup = Persist.newTable();

	@SuppressWarnings("unused")
	private NodeInfo backupSucc = null;

	public synchronized String[] get(String k) {
		List<String> vl = dict.get(k);
		if (vl == null) {
			return null;
		} else {
			String[] va = new String[vl.size()];
			return vl.toArray(va);
		}
	}

	public synchronized void add(String k, String v) {
		List<String> vl = dict.get(k);
		if (vl == null) {
			vl = new ArrayList<String>();
			dict.put(k, vl);
		}
		vl.add(v);
		// broadcast an event to any listeners
		broadcastAddition(k, v);
	}

	public synchronized void delete(String k, String v) {
		List<String> vs = dict.get(k);
		if (vs != null)
			vs.remove(v);
	}

	public synchronized void clear() {
		dict.clear();
	}

	/*
	 * Operations for transferring state between predecessor and successor.
	 */

	/*
	 * Successor: Extract the bindings from the successor node.
	 */
	public synchronized TableRep extractBindings(int predId) {
		return Persist.extractBindings(predId, info, successor, dict);
	}

	public synchronized TableRep extractBindings() {
		return Persist.extractBindings(info, successor, dict);
	}

	/*
	 * Successor: Drop the bindings that are transferred to the predecessor.
	 */
	public synchronized void dropBindings(int predId) {
		Persist.dropBindings(dict, predId, getNodeInfo().id);
	}

	/*
	 * Predecessor: Install the transferred bindings.
	 */
	public synchronized void installBindings(TableRep db) {
		dict = Persist.installBindings(dict, db);
	}

	/*
	 * Predecessor: Back up bindings from the successor.
	 */
	public synchronized void backupBindings(TableRep db) {
		backup = Persist.backupBindings(db);
		// backupSucc = db.getSucc();
	}

	public synchronized void backupSucc(TableRep db) {
		backupSucc = db.getSucc();
	}

	/*
	 * A never-used operation for storing state in a file.
	 */
	public synchronized void backup(String filename) throws IOException {
		Persist.save(info, successor, dict, filename);
	}

	public synchronized void reload(String filename) throws IOException {
		dict = Persist.load(filename);
	}

	public synchronized void display() {
		PrintWriter wr = new PrintWriter(System.out);
		Persist.display(dict, wr);
	}

	/*
	 * Routing operations.
	 */

	private NodeInfo predecessor = null;
	private NodeInfo successor = null;

	private NodeInfo[] finger;

	public synchronized void setPred(NodeInfo pred) {
		predecessor = pred;
	}

	public NodeInfo getPred() {
		return predecessor;
	}

	public synchronized void setSucc(NodeInfo succ) {
		successor = succ;
	}

	public NodeInfo getSucc() {
		return successor;
	}

	public synchronized void setFinger(int i, NodeInfo info) {
		/*
		 * TODO: Set the ith finger.
		 */
		finger[i] = info;
	}

	public synchronized NodeInfo getFinger(int i) {
		//return backupSucc;
		/*
		 * TODO: Get the ith finger.
		 */
		return finger[i];
	}

	public synchronized NodeInfo closestPrecedingFinger(int id) {
		//return backupSucc;
		/*
		 * TODO: Get closest preceding finger for id, to continue search at that
		 * node. Hint: See DHTBase.inInterval()
		 */
		
		/*
		if (includeUB && id==UB) 
			return true;
		if (LB < UB) {
			UB = (UB - LB);
			id = (id - LB);
			return 0 < id && id < UB;
		} else if (UB < LB) {
			UB = (UB + (IRouting.NKEYS - LB)) % IRouting.NKEYS;
			id = (id + (IRouting.NKEYS - LB)) % IRouting.NKEYS;
			return 0 < id && id < UB;
		} else {
			return false;
		} */
		
		int RoutingFingers = IRouting.NFINGERS;
		
		while (RoutingFingers > 0) {
			
			int RoutingKeys = IRouting.NKEYS;
			int Upper = finger[RoutingFingers].id % RoutingKeys;
			int Lower = finger[RoutingFingers - 1].id % RoutingKeys;
			
			if (id == Upper) {
				return getFinger(RoutingFingers -1);
			}
			
			if (Lower < Upper) {
				Upper = (Upper - Lower);
				Lower = (id - Lower);
				if (id > 0 && Upper > id) {
					return getFinger(RoutingFingers - 1);
				}
			} else if (Lower > Upper) {
				Upper = (Upper + RoutingKeys - Lower) % RoutingKeys;
				Lower = (id + RoutingKeys - Lower) % RoutingKeys;
				if (id > 0 && Upper > id) {
					return getFinger(RoutingFingers -1);
				}
			}
			
			RoutingFingers--;
		}
		
		return this.info;

	}

	public synchronized void routes() {
		PrintWriter wr = new PrintWriter(System.out);
		wr.println("Predecessor: " + predecessor);
		wr.println("Successor  : " + successor);
		wr.println("Fingers:");
		wr.printf("%7s  %3s  %s\n", "Formula", "Key", "Succ");
		wr.printf("%7s  %3s  %s\n", "-------", "---", "----");
		for (int i = 0, exp = 1; i < IRouting.NFINGERS; i++, exp = 2 * exp) {
			wr.printf(" %2d+2^%1d  %3d  [id=%2d,uri=%s]%n", info.id, i, (info.id + exp) % IRouting.NKEYS, finger[i].id,
					finger[i].addr);
		}
		wr.flush();
	}
	
	
	/*
	 * Used to prevent a race condition in the join protocol.
	 */
	
	public static enum JoinState {
		NOT_JOINED,
		JOINING,
		JOINED
	}
	
	private JoinState joinState = JoinState.NOT_JOINED;
	
	private Lock joinStateLock = new ReentrantLock();
	
	private Condition joined = joinStateLock.newCondition();
	
	public void startJoin() {
		joinStateLock.lock();
		try {
			joinState = JoinState.JOINING;
		} finally {
			joinStateLock.unlock();
		}
	}
	
	public void joinCheck() {
		// Called by any operations that should block during join protocol.
		// Currently that is getPred() (for the case where we are joining a 
		// single-node network).
		joinStateLock.lock();
		try {
			while (joinState == JoinState.JOINING) {
				joined.await();
			}
		} catch (InterruptedException e) {
			log.info("Join check loop was interrupted.");
		} finally {
			joinStateLock.unlock();
		}
	}
	
	public void finishJoin() {
		joinStateLock.lock();
		try {
			joinState = JoinState.JOINED;
			joined.signalAll();
		} finally {
			joinStateLock.unlock();
		}
	}
	
	/*
	 * Server-side listeners for new bindings.
	 */
	
private Map<String,SseBroadcaster> listeners = new HashMap<String,SseBroadcaster>();
	
	private Map<Integer,Map<String,EventOutput>> outputs = new HashMap<Integer,Map<String,EventOutput>>();
	
	public void removeListener(int clientId, String key) {
		// TODO Close the event output stream.
		//remove broadcaster
		//remove the key for client side
		
//		listeners.remove(key);
		if(outputs.get(clientId) == null) {
			return;
		}
		
		//close event output stream
		EventOutput output = outputs.get(clientId).get(key);
		if(output == null) {
			return;
		}
		
		if(output != null) {
			try {
				output.close();
			} catch (IOException e) {
				log.warning("Error Happens when try to close event generation in the server with id=" + clientId + ", key=" + key);
			}
		}
		
		SseBroadcaster caster = listeners.get(key);
		if(caster != null) {
			caster.remove(output);
		}
		
		outputs.get(clientId).remove(key);

	}
	
	private void broadcastAddition(String key, String value) {
		// TODO broadcast an added binding (use IDHTNode.NEW_BINDING_EVENT for event name).
		OutboundEvent.Builder builder = new OutboundEvent.Builder();
		OutboundEvent event = builder.name(IDHTNode.NEW_BINDING_EVENT)
									 .mediaType(MediaType.TEXT_PLAIN_TYPE)
									 .data(String.class, value)
									 .build();
		if(listeners.get(key) != null) {
			listeners.get(key).broadcast(event);
		}
	}
	
	/*
	 * Client-side callbacks for new binding notifications.
	 */
	
	private Map<String,EventSource> callbacks = new HashMap<String,EventSource>();
	
	private Map<String, NodeInfo> serverNodes = new HashMap<String,NodeInfo>();
	
	public void addCallback(String key, EventSource is, NodeInfo serverNode) {
		removeCallback(key);
		removeListenNode(key);
		callbacks.put(key, is);
		serverNodes.put(key, serverNode);
	}
	
	private void removeListenNode(String key) {
		serverNodes.remove(key);
	}
	
	public void removeCallback(String key) {
		// TODO remove an existing callback (if any) for bindings on key.
		// Be sure to close the event stream from the broadcaster.
		EventSource is = callbacks.get(key);
		if(is != null) {
			is.close();
			callbacks.remove(key);
			callbacks.put(key, BLANK_ENVENT_SOURCE);
		}
		
	}
	public void addListener(int clientId, String key, EventOutput os) {
		// TODO Auto-generated method stub
		// create event output binds
		Map<String, EventOutput> osforid = outputs.get(clientId);
		if(osforid == null) {
			osforid = new HashMap<String, EventOutput>();
			outputs.put(clientId, osforid);
		}
		osforid.put(key, os);
		
		//add to broadcaster
		SseBroadcaster broadcaster = listeners.get(key);
		if(broadcaster == null) {
			broadcaster = new SseBroadcaster();
			listeners.put(key, broadcaster);
		}
		broadcaster.add(os);
	}
	private void broadcastChangeBindings(String key) {
		// TODO broadcast an added binding (use IDHTNode.NEW_BINDING_EVENT for event name).
		OutboundEvent.Builder builder = new OutboundEvent.Builder();
		OutboundEvent event = builder.name(IDHTNode.CHANGE_BINDING_EVENT)
									 .mediaType(MediaType.TEXT_PLAIN_TYPE)
									 .data(String.class, key)
									 .build();
		if(listeners.get(key) != null) {
			listeners.get(key).broadcast(event);
		}
	}
	public void listCallbacks() {
		PrintWriter wr = new PrintWriter(System.out);
		if (callbacks.isEmpty()) {
			wr.println("No listeners defined.");
		} else {
			wr.println("Listeners defined for:");
			wr.printf("%9s  %2s  %s\n", "KEYSTRING", "KID", "StoreNode");
			List<String> removeCallbackKeys = new ArrayList<String>();
			List<String> removeServerNodeKeys = new ArrayList<String>();
			for (Entry<String, EventSource> entry : callbacks.entrySet()) {
				if (entry.getValue() != null && entry.getValue().isOpen()) {
					wr.printf("%9s  %3d  %9s\n",  entry.getKey(), DHTBase.NodeKey( entry.getKey()), "dht(" + serverNodes.get(entry.getKey()).id + ")");
				} else {
					wr.printf("%9s  %3d  %9s  %9s\n",  entry.getKey(), DHTBase.NodeKey( entry.getKey()), "dht(" + serverNodes.get(entry.getKey()).id + ")", "(closed)");
					removeCallbackKeys.add(entry.getKey());
					removeServerNodeKeys.add(entry.getKey());
				}
			}
			for(String tmp : removeCallbackKeys) {
				callbacks.remove(tmp);
			}
			for(String tmp : removeServerNodeKeys) {
				serverNodes.remove(tmp);
			}
		}
		wr.flush();
	}

	
	
	public void fixListener(int predId) {
		if(outputs.size() == 0) {
			return;
		}
		// avoid multiple background thread conflict with main thread
		synchronized (this) {
			// find entry contain key
			for(Entry<Integer,Map<String,EventOutput>> entry : outputs.entrySet()) {
				if(entry.getValue() == null || entry.getValue().size() == 0) {
					continue;
				}
				for(String skey : entry.getValue().keySet()) {
					if(skey != null && !DHTBase.inInterval(DHTBase.NodeKey(skey), predId, getNodeInfo().id) || predId == -1) {
						// notice its client to renew listen node
						broadcastChangeBindings(skey);
						//stop its event generation in the server node
						removeListener(entry.getKey(), skey);
						break;
					}
				}
			} 
		}
	}
	
	
	


	
}
