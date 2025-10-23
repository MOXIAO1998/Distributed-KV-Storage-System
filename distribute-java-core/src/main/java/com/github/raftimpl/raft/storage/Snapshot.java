package com.github.raftimpl.raft.storage;

import com.github.raftimpl.raft.proto.RaftProto;
import com.github.raftimpl.raft.util.RaftFileUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/* Snapshot
*  Description: the Snapshot mechanism is to implement isolation,
*  i.e preventing dirty write, by adding lock, but no lock when we read
*  the principle is "Read does not block write, and write does not block read"
* */

public class Snapshot {

    /* class of data structure
    * String: store fileName
    * RandomAccessFile: no need to read whole file,
        * RandomAccessFile raf = new RandomAccessFile("snapshot.data", "r");
        * raf.seek(500 * 1024 * 1024); // jump to byte 500 MB
        * byte[] buf = new byte[1024];
        * raf.read(buf); // read just 1 KB from there
    */
    public class SnapshotDataFile {
        public String fileName;
        public RandomAccessFile randomAccessFile;
    }

    private static final Logger LOG = LoggerFactory.getLogger(Snapshot.class);
    // snapshotDir: a directory path where this Raft node stores all its snapshot-related files.
    private String snapshotDir;
    private RaftProto.SnapshotMetaData metaData;

    // representing whether snapshot is being installed，leader lets follower to install，
    // leader and follower are installSnapshot status at the same time
    private AtomicBoolean isInstallSnapshot = new AtomicBoolean(false);

    // representing whether the node is snapshoting status machine
    private AtomicBoolean isTakeSnapshot = new AtomicBoolean(false);
    private Lock lock = new ReentrantLock();

    // Constructor function
    public Snapshot(String raftDataDir) {
        this.snapshotDir = raftDataDir + File.separator + "snapshot";
        String snapshotDataDir = snapshotDir + File.separator + "data";
        File file = new File(snapshotDataDir);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    // function to help object load/reset the in-memory metadata for the current snapshot
    public void reload() {
        // load if exists
        metaData = this.readMeta();
        // reset if not exist
        if (metaData == null) {
            metaData = RaftProto.SnapshotMetaData.newBuilder().build();
        }
    }

    /*
     * Open files under snapshot data content，
     * If it is a soft link, the actual file handle needs to be opened
     * @return map of file name and file handle
     */
    public TreeMap<String, SnapshotDataFile> openSnapshotFiles() {
        TreeMap<String, SnapshotDataFile> snapshotDataFileMap = new TreeMap<>();
        String snapshotDataDir = snapshotDir + File.separator + "data";
        try {
            // get path of snapshotDataDir
            Path snapshotDataPath = FileSystems.getDefault().getPath(snapshotDataDir);
            // get real path of snapshotDataPath
            snapshotDataPath = snapshotDataPath.toRealPath();
            // convert to string format of snapshotDataPath path
            snapshotDataDir = snapshotDataPath.toString();
            // List of files in snapshotDataDir after sorting
            List<String> fileNames = RaftFileUtils.getSortedFilesInDir(snapshotDataDir, snapshotDataDir);
            // Iterate the list of file, open and store it in a SnapshotDataFile instance
            for (String fileName : fileNames) {
                RandomAccessFile randomAccessFile = RaftFileUtils.openFile(snapshotDataDir, fileName, "r");
                SnapshotDataFile snapshotFile = new SnapshotDataFile();
                snapshotFile.fileName = fileName;
                snapshotFile.randomAccessFile = randomAccessFile;
                snapshotDataFileMap.put(fileName, snapshotFile);
            }
        } catch (IOException ex) {
            LOG.warn("readSnapshotDataFiles exception:", ex);
            throw new RuntimeException(ex);
        }
        return snapshotDataFileMap;
    }


    public void closeSnapshotFiles(TreeMap<String, SnapshotDataFile> snapshotDataFileMap) {
        for (Map.Entry<String, SnapshotDataFile> entry : snapshotDataFileMap.entrySet()) {
            try {
                entry.getValue().randomAccessFile.close();
            } catch (IOException ex) {
                LOG.warn("close snapshot files exception:", ex);
            }
        }
    }


    public RaftProto.SnapshotMetaData readMeta() {
        String fileName = snapshotDir + File.separator + "metadata";
        File file = new File(fileName);
        try( RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r") ) {
            RaftProto.SnapshotMetaData metadata = RaftFileUtils.readProtoFromFile(randomAccessFile, RaftProto.SnapshotMetaData.class);
            return metadata;
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", fileName);
            return null;
        }
    }

    // the function writes a new snapshot metadata file to disk
    // @parameter:
    // lastIncludedIndex:  Highest log index included in this snapshot.
    // lastIncludedTerm: Raft term of that log entry.
    // configuration: Cluster members when snapshot was taken.
    public void updateMeta(String dir, Long lastIncludedIndex, Long lastIncludedTerm, RaftProto.Configuration configuration) {
        // data knows which log entries can be discarded (they’re already in snapshot) and What the cluster looked like at that point.
        RaftProto.SnapshotMetaData snapshotMetaData = RaftProto.SnapshotMetaData.newBuilder()
                .setLastIncludedIndex(lastIncludedIndex)
                .setLastIncludedTerm(lastIncludedTerm)
                .setConfiguration(configuration)
                .build();

        String snapshotMetaFile = dir + File.separator + "metadata";
        RandomAccessFile randomAccessFile = null;


        try {
            File dirFile = new File(dir);
            if (!dirFile.exists()) {
                dirFile.mkdirs();
            }

            File file = new File(snapshotMetaFile);

            if (file.exists()) {
                FileUtils.forceDelete(file);
            }

            file.createNewFile();

            randomAccessFile = new RandomAccessFile(file, "rw");

            RaftFileUtils.writeProtoToFile(randomAccessFile, snapshotMetaData);
        } catch (IOException ex) {
            LOG.warn("meta file not exist, name={}", snapshotMetaFile);
        } finally {
            RaftFileUtils.closeFile(randomAccessFile);
        }
    }

    public RaftProto.SnapshotMetaData getMeta() {
        return metaData;
    }

    public String getSnapshotDir() {
        return snapshotDir;
    }

    public AtomicBoolean getIsinstallSnap() {
        return isInstallSnapshot;
    }

    public AtomicBoolean getIsTakeSnap() {
        return isTakeSnapshot;
    }

    public Lock getLock() {
        return lock;
    }
}
