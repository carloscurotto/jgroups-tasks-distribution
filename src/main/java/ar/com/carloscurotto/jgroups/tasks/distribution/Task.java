package ar.com.carloscurotto.jgroups.tasks.distribution;

import java.io.Serializable;

public interface Task extends Serializable {
	
	public  abstract  Object  execute();

}
