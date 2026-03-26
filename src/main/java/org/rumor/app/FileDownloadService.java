package org.rumor.app;

import org.rumor.gossip.NodeId;
import org.rumor.service.OnStateChange;
import org.rumor.service.RService;
import org.rumor.service.ServiceResponse;
import org.rumor.service.StatePublisher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Streams a file from this peer to the requester, and publishes the
 * local shared-file listing into gossip state so peers can discover
 * files without a separate discovery request.
 *
 * <p><b>State published</b> (key {@code SHARED_FILES}):
 * comma-separated entries of {@code name:size} for every file in the
 * shared root (e.g. {@code "report.pdf:1024,model.bin:5242880"}).
 *
 * <p><b>Request format</b> (UTF-8): filename within the shared root.
 * <p><b>Response</b>: raw file bytes, streamed in chunks.
 */
public class FileDownloadService extends RService implements StatePublisher {

    public static final String STATE_KEY = "SHARED_FILES";
    private static final int READ_BUFFER_SIZE = 64 * 1024;

    private final Path sharedRoot;

    public FileDownloadService(Path sharedRoot) {
        this.sharedRoot = sharedRoot.toAbsolutePath().normalize();
    }

    // --- StatePublisher ---

    @Override
    public String stateKey() {
        return STATE_KEY;
    }

    @Override
    public String computeState() {
        if (!Files.isDirectory(sharedRoot)) return "";

        StringJoiner joiner = new StringJoiner(",");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sharedRoot)) {
            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    String name = sharedRoot.relativize(entry).toString();
                    long size = Files.size(entry);
                    joiner.add(name + ":" + size);
                }
            }
        } catch (IOException e) {
            return "";
        }
        return joiner.toString();
    }

    // --- Client-side helpers ---

    /**
     * Returns remote peers' shared file listings from gossip state.
     *
     * @return map of node ID → file listing string (comma-separated name:size)
     */
    public Map<NodeId, String> discoverFiles() {
        return clusterView().stateForKey(STATE_KEY);
    }

    /**
     * Downloads a file from a peer that has it, selected automatically
     * by the framework (alive + offers this service + has the file).
     *
     * @param fileName      the file name to download
     * @param onStateChange callback for request lifecycle events
     */
    public void downloadFrom(String fileName, OnStateChange onStateChange) {
        byte[] request = fileName.getBytes(StandardCharsets.UTF_8);
        dispatch(request, onStateChange, appState -> {
            String files = appState.get(STATE_KEY);
            if (files == null || files.isEmpty()) return false;
            for (String entry : files.split(",")) {
                String name = entry.contains(":") ? entry.substring(0, entry.lastIndexOf(':')) : entry;
                if (name.equals(fileName)) return true;
            }
            return false;
        });
    }

    // --- Server-side: stream file bytes ---

    @Override
    public void serve(byte[] request, ServiceResponse response) {
        String fileName = new String(request, StandardCharsets.UTF_8).trim();
        Path filePath = sharedRoot.resolve(fileName).normalize();

        if (!filePath.startsWith(sharedRoot)) {
            response.write("ERROR: path outside shared root".getBytes(StandardCharsets.UTF_8));
            response.close();
            return;
        }

        if (!Files.isRegularFile(filePath)) {
            response.write("ERROR: file not found".getBytes(StandardCharsets.UTF_8));
            response.close();
            return;
        }

        try (InputStream in = Files.newInputStream(filePath)) {
            byte[] buf = new byte[READ_BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buf)) != -1) {
                if (bytesRead == buf.length) {
                    response.write(buf);
                } else {
                    byte[] chunk = new byte[bytesRead];
                    System.arraycopy(buf, 0, chunk, 0, bytesRead);
                    response.write(chunk);
                }
            }
            response.close();
        } catch (IOException e) {
            response.write(("ERROR: " + e.getMessage()).getBytes(StandardCharsets.UTF_8));
            response.close();
        }
    }
}
