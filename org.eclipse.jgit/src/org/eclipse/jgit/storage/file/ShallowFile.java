package org.eclipse.jgit.storage.file;

import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.internal.storage.file.LockFile;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.util.FileUtils;
import org.eclipse.jgit.util.IO;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShallowFile {
    private final File shallowFile;
    private final LockFile shallowLockFile;

    private final List<ObjectId> commitIds;

    /***
     * Constructor.<br/>
     * <br/>
     * No reading or locking is done here.
     *
     * @param parentDirectory
     *            $GITDIR meta-data directory
     */
    public FileBasedShallow(final File parentDirectory) {
        this.commitIds = new ArrayList<>();
        if (parentDirectory == null) {
            // should never get here
            throw new RuntimeException(
                    JGitText.get().repositoryLocalMetadataDirectoryInvalid);
        }
        shallowFile = new File(parentDirectory, Constants.SHALLOW);
        shallowLockFile = new LockFile(shallowFile);
    }


    public void lock() throws IOException {
        if (!shallowLockFile.lock()) {
            final String path = shallowFile.getAbsolutePath();
            throw new RepositoryShallowException(JGitText.get().cannotLock,
                    path);
        }
    }

    public List<ObjectId> read() throws IOException {
        commitIds.clear();
        if (!shallowFile.exists()) {
            // file does not exist which means no shallow commits and,
            // therefore, nothing to do here
            return Collections.emptyList();
        }
        try (final FileReader in = new FileReader(shallowFile)) {
            for (String line;;) {
                // last char is for new-line
                line = IO.readLine(in, HASH_LENGTH + 1);
                if (line.length() == 0) {
                    break;
                }
                final ObjectId id = convertStringToObjectId(line, 0,
                        HASH_LENGTH);
                commitIds.add(id);
            }
        }
        return Collections.unmodifiableList(commitIds);
    }

    public boolean parseShallowUnshallowLine(final String line)
            throws IOException {
        final int length = line.length();
        if (length == 0) {
            return false;
        }
        if (line.startsWith(PREFIX_SHALLOW)) {
            final ObjectId objId = convertStringToObjectId(line,
                    PREFIX_SHALLOW.length(), PREFIX_SHALLOW.length() + 40);
            commitIds.add(objId);
        } else if (line.startsWith(PREFIX_UNSHALLOW)) {
            final ObjectId objId = convertStringToObjectId(line,
                    PREFIX_UNSHALLOW.length(), PREFIX_UNSHALLOW.length() + 40);
            commitIds.remove(objId);
        } else {
            throw new RepositoryShallowException(
                    JGitText.get().expectedShallowUnshallowGot, line);
        }
        return true;
    }

    public void unlock(final boolean writeChanges) throws IOException {
        if (writeChanges) {
            try {
                writeChanges();
            } catch (IOException ex) {
                ex.printStackTrace();
            } finally {
                shallowLockFile.unlock();
            }
        } else {
            shallowLockFile.unlock();
        }
    }

    private void writeChanges() throws IOException {
        if (commitIds.isEmpty()) {
            if (shallowFile.exists()) {
                // no shallow commits, so whole repository
                // is not shallow anymore
                FileUtils.delete(shallowFile);
                shallowLockFile.unlock();
            }
            return;
        }
        final StringBuffer buffer = new StringBuffer();
        Collections.sort(commitIds);
        for (ObjectId id : commitIds) {
            // shallowLockFile.write(id); is not able to write more than a
            // single id!!!
            buffer.append(id.name());
            buffer.append('\n');
        }
        final String contentAsString = buffer.toString();
        final byte[] content = contentAsString.getBytes();
        shallowLockFile.write(content);
        shallowLockFile.commit();
    }
}
