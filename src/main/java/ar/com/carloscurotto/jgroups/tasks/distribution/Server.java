package ar.com.carloscurotto.jgroups.tasks.distribution;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.util.Promise;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

public class Server extends ReceiverAdapter implements Master, Slave {
	
	private static final Log LOGGER = LogFactory.getLog(Server.class);

	private String props = "udp.xml";
	private Channel ch;
	private final ConcurrentMap<ClusterID, Entry> tasks = new ConcurrentHashMap<ClusterID, Entry>();
	private final ExecutorService thread_pool = Executors.newCachedThreadPool();
	private View view;
	private int rank = -1;
	private int cluster_size = -1;
	
	public static void main(String[] args) throws Exception {
		Server server = new Server();
		server.start();
		loop(server);
	}

	private static void loop(Server server) throws Exception {
		boolean looping = true;
		Object result = null;
		while (looping) {
			int key = Util
					.keyPress("[1]  Submit  [2]  Submit  long  running  task  [q]  Quit");
			switch (key) {
			case '1':
				Task task = new Task() {
					private static final long serialVersionUID = 1L;
					public Object execute() {
						return new Date();
					}
				};
				result = server.submit(task, 30000);
				LOGGER.info("<==  result  =  " + result);
				break;
			case '2':
				task = new Task() {
					private static final long serialVersionUID = 1L;
					public Object execute() {
						Util.sleep(15000);
						return new Date();
					}
				};
				result = server.submit(task, 30000);
				LOGGER.info("<==  result  =  " + result);
				break;
			}
		}
		server.stop();
	}

	public void start() throws Exception {
		ch = new JChannel(props);
		ch.setReceiver(this);
		ch.connect("dzone-demo");
		LOGGER.info("My rank is " + rank);
		LOGGER.info(ch.getView().toString());
	}

	public void stop() {
		thread_pool.shutdown();
		ch.close();
	}

	@Override
	public Object handle(Task task) {
		return task.execute();
	}

	@Override
	public Object submit(Task task, long timeout) throws Exception {
		ClusterID id = ClusterID.create(ch.getAddress());
		try {
			Request req = new Request(Request.Type.EXECUTE, task, id, null);
			byte[] buf = Util.streamableToByteBuffer(req);
			Entry entry = new Entry(task, ch.getAddress());
			tasks.put(id, entry);
			ch.send(new Message(null, null, buf));
			return entry.promise.getResultWithTimeout(timeout);
		} catch (Exception ex) {
			tasks.remove(id); // remove it again
			throw ex;
		}
	}

	public void receive(Message msg) {
		try {
			Request req = (Request) Util.streamableFromByteBuffer(
					Request.class, msg.getBuffer());
			switch (req.type) {
			case EXECUTE:
				//LOGGER.info("Executing task [" + req.id.getCreator() + ":" + req.id.getId() + "]");
				handleExecute(req.id, msg.getSrc(), req.task);
				break;
			case RESULT:
				Entry entry = tasks.get(req.id);
				//LOGGER.info("Receiving result [" + req.result + "] for task [" + req.id.getCreator() + ":" + req.id.getId() + "]");
				entry.promise.setResult(req.result);
				multicastRemoveRequest(req.id);
				break;
			case REMOVE:
				//LOGGER.info("Removing task [" + req.id.getCreator() + ":" + req.id.getId() + "]");
				tasks.remove(req.id);
				break;
			}
		} catch (Exception e) {
			throw new RuntimeException("Error receivig a message.", e);
		}
	}

	private void multicastRemoveRequest(ClusterID id) {
		try {
			Request response = new Request(Request.Type.REMOVE, null, id, null);
			byte[] buf = Util.streamableToByteBuffer(response);
			ch.send(new Message(id.getCreator(), null, buf));
		} catch (Exception e) {
			throw new RuntimeException("Error multicasting remove message.", e);
		}
	}

	private void handleExecute(ClusterID id, Address sender, Task task) {
		tasks.putIfAbsent(id, new Entry(task, sender));
		int index = id.getId() % cluster_size;
		if (index != rank)
			return;
		thread_pool.execute(new Handler(id, sender, task));
	}

	public void viewAccepted(View view) {
		List<Address> left_members = Util.leftMembers(this.view, view);
		this.view = view;
		Address local_addr = ch.getAddress();
		cluster_size = view.size();
		List<Address> mbrs = view.getMembers();
		for (int i = 0; i < mbrs.size(); i++) {
			Address tmp = mbrs.get(i);
			if (tmp.equals(local_addr)) {
				rank = i;
				break;
			}
		}
		if (left_members != null && !left_members.isEmpty()) {
			for (Address mbr : left_members)
				handleLeftMember(mbr);
		}
	}

	private void handleLeftMember(Address mbr) {
		for (Map.Entry<ClusterID, Entry> entry : tasks.entrySet()) {
			ClusterID id = entry.getKey();
			int index = id.getId() % cluster_size;
			if (index != rank)
				return;
			Entry val = entry.getValue();
			if (mbr.equals(val.submitter)) {
				continue;
			}
			handleExecute(id, val.submitter, val.task);
		}
	}

	private class Handler implements Runnable {

		final ClusterID id;
		final Address sender;
		final Task task;

		public Handler(ClusterID id, Address sender, Task task) {
			this.id = id;
			this.sender = sender;
			this.task = task;
		}

		public void run() {
			try {
				Object result = null;
				try {
					LOGGER.info("Executing task [" + id.getCreator() + ":" + id.getId() + "]");
					result = handle(task);
				} catch (Throwable t) {
					result = t;
				}
				Request response = new Request(Request.Type.RESULT, null, id,
						result);
				byte[] buf = Util.streamableToByteBuffer(response);
				ch.send(new Message(sender, null, buf));
			} catch (Exception e) {
				throw new RuntimeException("Error handling task.", e);
			}
		}
	}

	private static class Entry {
		private final Task task;
		private final Address submitter;
		private final Promise<Object> promise = new Promise<Object>();

		public Entry(Task task, Address submitter) {
			this.task = task;
			this.submitter = submitter;
		}
	}

	public static class Request implements Streamable {

		static enum Type {
			EXECUTE, RESULT, REMOVE
		};

		private Type type;
		private Task task;
		private ClusterID id;
		private Object result;
		
		public Request() {}

		public Request(Type type, Task task, ClusterID id, Object result) {
			this.type = type;
			this.task = task;
			this.id = id;
			this.result = result;
		}

		@Override
		public void writeTo(DataOutput out) throws Exception {
			Util.writeString(this.type.name(), out);
			Util.writeObject(this.task, out);
			Util.writeStreamable(this.id, out);
			Util.writeObject(this.result, out);
		}

		@Override
		public void readFrom(DataInput in) throws Exception {
			this.type = Type.valueOf(Util.readString(in));
			this.task = (Task) Util.readObject(in);
			this.id = (ClusterID) Util.readStreamable(ClusterID.class, in);
			this.result = Util.readObject(in);
		}
	}

}
