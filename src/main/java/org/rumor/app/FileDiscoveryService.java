package org.rumor.app;

import org.rumor.service.RService;
import org.rumor.service.ServiceResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Lists files available for download on this peer.
 *
 * <p>Request: empty (lists the shared root).
 * <p>Response format (UTF-8): one line per entry —
 * {@code FILE <size_bytes> <relative_path>} or {@code DIR <relative_path>}.
 */
public class FileDiscoveryService extends RService {

    private final Path sharedRoot;

    public FileDiscoveryService(Path sharedRoot) {
        this.sharedRoot = sharedRoot.toAbsolutePath().normalize();
    }

    public Path sharedRoot() {
        return sharedRoot;
    }

    @Override
    public void serve(byte[] request, ServiceResponse response) {
        if (!Files.isDirectory(sharedRoot)) {
            response.write(("ERROR: shared directory does not exist: " + sharedRoot + "\n")
                    .getBytes(StandardCharsets.UTF_8));
            response.close();
            return;
        }

        StringBuilder listing = new StringBuilder();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(sharedRoot)) {
            for (Path entry : stream) {
                String relativePath = sharedRoot.relativize(entry).toString();
                if (Files.isDirectory(entry)) {
                    listing.append("DIR  ").append(relativePath).append('\n');
                } else {
                    long size = Files.size(entry);
                    listing.append("FILE ").append(size).append(' ').append(relativePath).append('\n');
                }
            }
        } catch (IOException e) {
            listing.append("ERROR: ").append(e.getMessage()).append('\n');
        }

        response.write(listing.toString().getBytes(StandardCharsets.UTF_8));
        response.close();
    }
}
