package ar.com.carloscurotto.jgroups.tasks.distribution;

import java.io.DataInput;
import java.io.DataOutput;

import org.jgroups.Address;
import org.jgroups.util.Streamable;
import org.jgroups.util.Util;

public class ClusterID implements Streamable {
	
	private  Address  creator;
    private   int  id;
    private  static  int  next_id=1;
    
    public ClusterID() {}
    
    private ClusterID(Address  creator, int id) {
    	this.creator = creator;
    	this.id = id;
    }
    
    public int getId() {
    	return id;
    }
    
    public Address getCreator() {
    	return creator;
    }
    
    public  static  synchronized  ClusterID  create(Address  addr)  {
        return  new  ClusterID(addr,  next_id++);
    }

	@Override
	public void writeTo(DataOutput out) throws Exception {
		Util.writeAddress(this.creator, out);
		out.writeInt(this.id);
	}

	@Override
	public void readFrom(DataInput in) throws Exception {
		this.creator = Util.readAddress(in);
		this.id = in.readInt();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((creator == null) ? 0 : creator.hashCode());
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ClusterID other = (ClusterID) obj;
		if (creator == null) {
			if (other.creator != null)
				return false;
		} else if (!creator.equals(other.creator))
			return false;
		if (id != other.id)
			return false;
		return true;
	}

}
