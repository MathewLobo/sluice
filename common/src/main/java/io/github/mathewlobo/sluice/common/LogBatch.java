package io.github.mathewlobo.sluice.common;

import java.util.List;

public record LogBatch(String agentId, long seq, long firstLine, List<String> lines) {

    public LogBatch{
        lines = List.copyOf(lines);
    }
}
