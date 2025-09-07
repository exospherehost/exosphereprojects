import json
import logging
import math
from exospherehost import BaseNode
from pydantic import BaseModel
from typing import List

# Configure logger for this node
logger = logging.getLogger(__name__)


class ChunkingNode(BaseNode):

    class Inputs(BaseModel):
        file_paths: str  # JSON string representation of list of file paths
        chunk_size: str  # Size of each chunk (default: "10")

    class Outputs(BaseModel):
        chunk: str  # JSON string representation of a chunk of file paths

    class Secrets(BaseModel):
        pass

    async def execute(self) -> List[Outputs]:
        """
        Create chunks of file paths for batch processing.
        """
        logger.info("Starting file path chunking process")
        
        # Parse JSON string to list
        try:
            file_paths = json.loads(self.inputs.file_paths)
            chunk_size = int(self.inputs.chunk_size)
            logger.info(f"Successfully parsed {len(file_paths)} file paths, chunk size: {chunk_size}")
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        # Create chunks
        chunks = []
        for i in range(0, len(file_paths), chunk_size):
            chunk = file_paths[i:i + chunk_size]
            chunks.append(chunk)
        
        logger.info(f"Created {len(chunks)} chunks")
        
        # Create outputs for each chunk
        outputs = []
        for i, chunk in enumerate(chunks):
            chunk_json = json.dumps(chunk)
            outputs.append(self.Outputs(
                chunk=chunk_json
            ))
            logger.info(f"Chunk {i+1}: {len(chunk)} files")
        
        logger.info(f"Successfully created {len(outputs)} chunks")
        print(f"Created {len(outputs)} chunks from {len(file_paths)} file paths")
        
        return outputs
