package ar.com.carloscurotto.jgroups.tasks.distribution;

public interface Master {
	
	Object  submit(Task  task,  long  timeout)  throws  Exception;

}
