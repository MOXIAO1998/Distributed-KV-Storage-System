package com.github.raftimpl.raft.example.server.machine;


import com.github.raftimpl.raft.RaftNode;
import com.github.raftimpl.raft.StateMachine;
import com.github.raftimpl.raft.example.server.service.ExampleProto;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/*
 * @Author: Moxiao Li
 * @Version: Aug 16, 2025
 */
public class RocksDBStateMachine implements StateMachine {
    // use Logger to record warning and info
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBStateMachine.class);
    // initialize RocksDB
    static{
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private final String raftDataDir;

    public RocksDBStateMachine(String raftDataDir) {
        this.raftDataDir = raftDataDir;
    }


    @Override
    public void writeSnap(String snapshotDir, String tmpSnapshotDataDir,
                          RaftNode raftNode, long localLastAppliedIndex) {

        Checkpoint checkpoint = Checkpoint.create(db);

        try {
            // the write action is written in createCheckpoint
            checkpoint.createCheckpoint(snapshotDir);
            LOG.info("Checkpoint was created");

        } catch (Exception e) {
            // use Log to record warn
            LOG.warn("writeSnapshot meet exception, dir={}, msg={}",
                    snapshotDir,e.getMessage());
        }


    }

    @Override
    public void readSnap(String snapshotDir) {
        try{
            // close db connection
            if (db != null) {
                db.close();
                db = null;
            }

            // path, node store rocksDB data
            String dataDir = raftDataDir + File.separator + "rocksdb_data";
            File dataFile = new File(dataDir);
            if (dataFile.exists()) {
                FileUtils.deleteDirectory(dataFile);
            }

            File snapshotFile = new File(snapshotDir);
            // copy files under snapshot into dataDir
            if (snapshotFile.exists()) {
                FileUtils.copyDirectory(snapshotFile,dataFile);
            }

            // reopen date base connection
            Options options = new Options();
            options.setCreateIfMissing(true);
            db = RocksDB.open(options, dataDir);

        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
        LOG.info("snapshot has been copied");

    }

    // synchronized state machine data
    @Override
    public void applyData(byte[] dataBytes) {
        try{
            ExampleProto.SetRequest request = ExampleProto.SetRequest.parseFrom(dataBytes);
            db.put(request.getKey().getBytes(), request.getValue().getBytes());
            LOG.info("data has been applied in RocksDB");
        } catch (Exception e) {
            LOG.warn("meet exception, msg={}", e.getMessage());
        }
    }

    // get data from state machine
    @Override
    public byte[] get(byte[] dataBytes) {
        byte[] result = null;

        try {
            byte[] valueBytes = db.get(dataBytes);
            if (valueBytes != null) {
                result = valueBytes;
            }


        } catch (Exception e) {
            LOG.warn("read rocksdb error, msg={}", e.getMessage());
        }
        LOG.info("data has been got");
        LOG.info("the key is: {}", new String(dataBytes));
        LOG.info("the value is: {}", new String(result));
        return result;
    }
}
