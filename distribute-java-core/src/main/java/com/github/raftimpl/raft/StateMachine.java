package com.github.raftimpl.raft;

public interface StateMachine {

    /**
     * snapshot the data in state machine, each node is called locally at regular intervals
     * @param snapshotDir old snapshot dir
     * @param tmpSnapshotDataDir new snapshot dir
     * @param raftNode Raft node
     * @param localLastAppliedIndex The maximum log entry index that has been applied to the replication state machine
     */
    void writeSnap(String snapshotDir, String tmpSnapshotDataDir, RaftNode raftNode, long localLastAppliedIndex);

    /**
     * Read the snapshot to the state machine and call it when the node starts
     * @param snapshotDir snapshot数据目录
     */
    void readSnap(String snapshotDir);

    /**
     * 将数据应用到状态机
     * @param dataBytes 数据二进制
     */
    void applyData(byte[] dataBytes);

    /**
     * read data from state machine
     * @param dataBytes Key data (binary)
     * @return Value data (binary)
     */
    byte[] get(byte[] dataBytes);
}
