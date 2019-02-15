package me.vukas;

public class PartitionId {
	private static final Character SEPARATOR = '_';
	private Integer number;
	private String resourceName;

	public Integer getNumber() {
		return number;
	}

	public String getResourceName() {
		return resourceName;
	}

	public String createReceivingRoutingKey(){
		return "*." + this.number;
	}

	public String createSendingRoutingKey(Integer totalPartitions){
		return totalPartitions + "." + this.number;
	}

	public static PartitionId fromPartitionNumber(Integer partitionNumber){
		PartitionId partitionId = new PartitionId();
		partitionId.number = partitionNumber;
		return partitionId;
	}

	public static PartitionId fromPartitionKey(String partitionKey){
		PartitionId partitionId = new PartitionId();
		partitionId.number = Integer.parseInt(partitionKey.substring(partitionKey.lastIndexOf(SEPARATOR)+1));
		partitionId.resourceName = partitionKey.substring(0, partitionKey.lastIndexOf(SEPARATOR));
		return partitionId;
	}

	@Override
	public String toString() {
		return this.resourceName + SEPARATOR + this.number;
	}
}
