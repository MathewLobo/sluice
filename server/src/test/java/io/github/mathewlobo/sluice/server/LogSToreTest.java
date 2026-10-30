package io.github.mathewlobo.sluice.server;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.mathewlobo.sluice.common.LogBatch;

class LogStoreTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;          // JUnit creates a fresh empty folder for each test and deletes it after

    // Helper: read every record the store wrote
    private List<LogBatch> readRecords(Path file) throws IOException {
        List<LogBatch> records = new ArrayList<>();
        for (String line : Files.readAllLines(file)) {
            if (!line.isBlank()) records.add(MAPPER.readValue(line, LogBatch.class));
        }
        return records;
    }

    @Test
    void newBatchIsStored() throws IOException {
        Path file = tempDir.resolve("logs.dat");

        try (LogStore store = new LogStore(file)) {
            boolean stored = store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3")));
            assertTrue(stored);
        }

        List<LogBatch> records = readRecords(file);
        assertEquals(1, records.size());
        assertEquals(1, records.get(0).firstLine());
        assertEquals(List.of("l1", "l2", "l3"), records.get(0).lines());
    }


    @Test 
    void exactDuplicateIsNotStored() throws IOException {
        Path file = tempDir.resolve("logs.dat");
        try (LogStore store = new LogStore(file)) {
            boolean stored = store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3")));
            assertTrue(stored);
            stored = store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3")));
            assertFalse(stored);
        }

        assertEquals(1, readRecords(file).size());
    }

    @Test 
    void partialOverlapStoresOnlyNewLines() throws IOException{

        Path file = tempDir.resolve("logs.dat");
        try (LogStore store = new LogStore(file)) {
            boolean stored = store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3", "l4")));
            assertTrue(stored);
            stored = store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3","l4","l5")));
            assertTrue(stored);
        }

        List<LogBatch> records = readRecords(file);
        assertEquals(2, records.size());
        assertEquals(1, records.get(0).firstLine());
        assertEquals(List.of("l1", "l2", "l3","l4"), records.get(0).lines());
        assertEquals(List.of("l5"), records.get(1).lines());
        assertEquals(5, records.get(1).firstLine());
        
    }

    @Test
    void duplicateAfterRestartIsNotStored() throws IOException {
        Path file = tempDir.resolve("logs.dat");
        try (LogStore store = new LogStore(file)) {
            assertTrue(store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3"))));
        }
        try (LogStore store = new LogStore(file)) {      // "restart": rebuilds state from the file
            assertFalse(store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3"))));
        }
        assertEquals(1, readRecords(file).size());
    }

    @Test
    void gapIsStoredAnyway() throws IOException {
        // expect: both appends store (true); lines 4-9 never arrived, so the server
        //         warns but still stores l10, l11 as a second record starting at line 10
        Path file = tempDir.resolve("logs.dat");
        try (LogStore store = new LogStore(file)) {
            assertTrue(store.append(new LogBatch("a1", 1, 1, List.of("l1", "l2", "l3"))));
            assertTrue(store.append(new LogBatch("a1", 2, 10, List.of("l10", "l11"))));
        }

        List<LogBatch> records = readRecords(file);
        assertEquals(2, records.size());
        assertEquals(10, records.get(1).firstLine());               
        assertEquals(List.of("l10", "l11"), records.get(1).lines());
    }



}