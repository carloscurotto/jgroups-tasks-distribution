package ar.com.carloscurotto.jgroups.tasks.distribution;

public interface Slave {
	
	Object  handle(Task  task);

}
