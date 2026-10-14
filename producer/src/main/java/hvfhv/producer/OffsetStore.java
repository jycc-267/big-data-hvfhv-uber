package hvfhv.producer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Persists how many source rows have been published, so a restart resumes where the
 * previous run stopped instead of replaying from the beginning (which double-counts in
 * the speed view). Writes are atomic: a crash never leaves a truncated file behind.
 */
public final class OffsetStore {

    private final Path file;

    public OffsetStore(Path file) {
        this.file = file;
    }

    /** The saved offset, or {@code initial} when nothing has been published yet. */
    public long load(long initial) {
        if (!Files.exists(file)) {
            return initial;
        }
        try {
            return Long.parseLong(Files.readString(file, StandardCharsets.UTF_8).trim());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read producer offset from " + file, e);
        }
    }

    public void save(long offset) {
        try {
            Path parent = file.toAbsolutePath().getParent();
            Files.createDirectories(parent);
            Path tmp = Files.createTempFile(parent, file.getFileName().toString(), ".tmp");
            Files.writeString(tmp, Long.toString(offset), StandardCharsets.UTF_8);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write producer offset to " + file, e);
        }
    }
}
