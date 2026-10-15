package io.github.mathewlobo.sluice.agent;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

public class CheckpointStore {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Path filePath;
    
    public CheckpointStore(Path filePath){
        this.filePath = Objects.requireNonNull(filePath, "filePath cannot be null");
    }

    public Checkpoint load() throws IOException{
        
        if (!Files.exists(filePath)){
    
            return new Checkpoint(0,0);
        }
        else{
            return mapper.readValue(filePath.toFile(), Checkpoint.class);
        }
    }

    public void save(Checkpoint cp) throws IOException{

        Path tmp = Path.of(filePath + ".tmp");
        
        String json = mapper.writeValueAsString(cp);
        Files.writeString(tmp, json, StandardCharsets.UTF_8);

        Files.move(tmp, filePath,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE);
    }

}

