package org.eclipse.jgit.storage.file;

import org.eclipse.jgit.annotations.NonNull;
import org.eclipse.jgit.errors.LockFailedException;
import org.eclipse.jgit.internal.JGitText;
import org.eclipse.jgit.internal.storage.file.LockFile;
import org.eclipse.jgit.lib.Constants;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.util.FileUtils;
import org.eclipse.jgit.util.IO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ShallowFile {
    /***
     * Shallow line format: <code>shallow &lt;commitId&gt;\n</code>
     */
    public static final String PREFIX_SHALLOW = "shallow "; //$NON-NLS-1$

    /***
     * Unshallow line format: <code>unshallow &lt;commit-SHA1-hash&gt;\n</code>
     */
    public static final String PREFIX_UNSHALLOW = "unshallow "; //$NON-NLS-1$

    private static final Logger log = LoggerFactory.getLogger(ShallowFile.class);

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
    public ShallowFile(@NonNull final File parentDirectory) {
        this.commitIds = new ArrayList<>();
        shallowFile = new File(parentDirectory, Constants.SHALLOW);
        shallowLockFile = new LockFile(shallowFile);
    }


    public void lock() throws IOException {
        if (!shallowLockFile.lock()) {
            throw new LockFailedException(shallowFile, JGitText.get().cannotLock);
        }
    }

    public List<ObjectId> read() throws IOException {
        commitIds.clear();
        if (!shallowFile.exists()) {
            return Collections.emptyList();
        }
        try (final FileReader in = new FileReader(shallowFile)) {
            for (String line;;) {
                // last char is for new-line
                line = IO.readLine(in, Constants.OBJECT_ID_STRING_LENGTH + 1);
                if (line.length() == 0) {
                    break;
                }
                final ObjectId id = convertStringToObjectId(line, 0,
                        Constants.OBJECT_ID_STRING_LENGTH);
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
                    JGitText.get().invalidShallowFile, line);
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

    protected ObjectId convertStringToObjectId(final String string,
                                               int beginIndex, int endIndex) {
        final String objectIdAsString = string.substring(beginIndex, endIndex);
        try {
             return ObjectId.fromString(objectIdAsString);
        } catch (IllegalArgumentException ex) {
                   log.error(MessageFormat.format(JGitText.get().badShallowLine,
                            objectIdAsString));
                   throw ex;
        }
    }

    protected class RepositoryShallowException extends IOException {

        /**
         * serialVersionUID generated by Eclipse
         */
        private static final long serialVersionUID = -6812015161109424512L;

        /***
         * Constructor for exception
         *
         * @param message
         */
        public RepositoryShallowException(final String message) {
            super(message);
        }

        /***
         * Constructor for exception
         *
         * @param title
         * @param argument
         */
        public RepositoryShallowException(final String title,
                                          final String argument) {
            super(MessageFormat.format(title, argument));
        }

    }
}
