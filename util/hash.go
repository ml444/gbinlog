package util

func GetMod4Shard(objectId uint64, shardCount int) int32 {
	return int32(objectId % uint64(shardCount))
}

func GetPartition(objectId uint64, shardCount int) int32 {
	mod := GetMod4Shard(objectId, shardCount)
	return mod
}
