package me.vukas;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;

public class PartitionMapper {
	private Integer totalPartitions = 6;

	public Integer getTotalPartitions() {
		return totalPartitions;
	}

	public <T> PartitionId mapToPartition(T property){
		HashCode hashCode = Hashing.murmur3_128().hashString(String.valueOf(property), Charsets.UTF_8);
		Integer partitionNumber = Hashing.consistentHash(hashCode, this.totalPartitions);
		return PartitionId.fromPartitionNumber(partitionNumber);
	}

	public <T> String mapToRoutingKey(T property){
		return this.mapToPartition(property).createSendingRoutingKey(this.totalPartitions);
	}
}
