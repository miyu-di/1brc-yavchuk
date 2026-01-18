/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package dev.morling.onebrc;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class CalculateAverage_diana {

    private static class StationStats {
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        double sum = 0;
        long count = 0;

        void update(double value) {
            min = Math.min(min, value);
            max = Math.max(max, value);
            sum += value;
            count++;
        }

        void combine(StationStats other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        @Override
        public String toString() {
            return String.format(Locale.US, "%.1f/%.1f/%.1f", min, Math.round(sum * 10.0 / count) / 10.0, max);
        }
    }

    public static void main(String[] args) throws Exception {
        var path = Paths.get("measurements.txt");
        var channel = FileChannel.open(path, StandardOpenOption.READ);
        long fileSize = channel.size();
        int cores = Runtime.getRuntime().availableProcessors();
        long chunkSize = fileSize / cores;

        var executor = Executors.newFixedThreadPool(cores);
        var futures = new ArrayList<Future<Map<String, StationStats>>>();

        for (int i = 0; i < cores; i++) {
            long start = i * chunkSize;
            long end = (i == cores - 1) ? fileSize : (i + 1) * chunkSize;
            futures.add(executor.submit(() -> processChunk(channel, start, end)));
        }

        var finalMap = new TreeMap<String, StationStats>();
        for (var future : futures) {
            future.get().forEach((station, stats) -> finalMap.computeIfAbsent(station, k -> new StationStats()).combine(stats));
        }

        executor.shutdown();
        System.out.println(finalMap.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", ", "{", "}")));
    }

    private static Map<String, StationStats> processChunk(FileChannel channel, long start, long end) throws IOException {
        var statsMap = new HashMap<String, StationStats>();
        MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, start, end - start);

        if (start > 0) {
            while (buffer.hasRemaining() && buffer.get() != '\n')
                ;
        }

        byte[] nameBuffer = new byte[100];
        while (buffer.hasRemaining()) {
            int namePos = 0;
            byte b;
            while (buffer.hasRemaining() && (b = buffer.get()) != ';') {
                nameBuffer[namePos++] = b;
            }
            if (!buffer.hasRemaining())
                break;

            String name = new String(nameBuffer, 0, namePos, java.nio.charset.StandardCharsets.UTF_8);

            double val = 0;
            boolean negative = false;
            while (buffer.hasRemaining()) {
                b = buffer.get();
                if (b == '-')
                    negative = true;
                else if (b >= '0' && b <= '9')
                    val = val * 10 + (b - '0');
                else if (b == '.') {
                }
                else if (b == '\n' || b == '\r')
                    break;
            }
            val = (negative ? -val : val) / 10.0;
            statsMap.computeIfAbsent(name, k -> new StationStats()).update(val);
        }
        return statsMap;
    }
}
