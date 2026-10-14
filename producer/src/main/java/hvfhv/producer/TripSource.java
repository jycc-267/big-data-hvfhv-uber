package hvfhv.producer;

import java.io.IOException;
import java.util.List;

/** An ordered, pageable source of trips. */
public interface TripSource {

    /** One page of trips, each as a compact JSON object string; empty once the range is exhausted. */
    List<String> fetchPage(long offset, int limit) throws IOException, InterruptedException;
}
