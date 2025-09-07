import json
import logging
from exospherehost import BaseNode
from pydantic import BaseModel
from typing import List

# Configure logger for this node
logger = logging.getLogger(__name__)


class FileDistributionNode(BaseNode):

    class Inputs(BaseModel):
        file_paths: str  # JSON string representation of list of file paths

    class Outputs(BaseModel):
        file_path: str  # JSON string representation of a single file path

    class Secrets(BaseModel):
        pass

    async def execute(self) -> List[Outputs]:
        """
        Distribute file paths into individual states for parallel processing.
        Each file will be processed separately by the sync processing node.
        """
        logger.info("Starting file distribution process")
        
        # Parse JSON string to list
        try:
            file_paths = json.loads(self.inputs.file_paths)
            logger.info(f"Successfully parsed {len(file_paths)} file paths")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse file paths: {e}")
            raise
        
        # Create individual outputs for each file
        outputs = []
        for i, file_path in enumerate(file_paths):
            file_path_json = json.dumps(file_path)
            outputs.append(self.Outputs(
                file_path=file_path_json
            ))
            logger.info(f"File {i+1}: {file_path}")
        
        logger.info(f"Successfully distributed {len(outputs)} files for individual processing")
        print(f"Distributed {len(outputs)} files for individual processing")
        
        return outputs
