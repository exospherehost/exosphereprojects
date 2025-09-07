import json
import logging
import pandas as pd
from exospherehost import BaseNode
from pydantic import BaseModel
from typing import List

# Configure logger for this node
logger = logging.getLogger(__name__)


class CSVInputNode(BaseNode):

    class Inputs(BaseModel):
        csv_file_path: str  # Path to the CSV file containing file paths

    class Outputs(BaseModel):
        file_paths: str  # JSON string representation of list of file paths

    class Secrets(BaseModel):
        pass

    async def execute(self) -> Outputs:
        """
        Read file paths from CSV input file.
        """
        logger.info(f"Starting CSV input processing for file: {self.inputs.csv_file_path}")
        
        try:
            # Read CSV file
            df = pd.read_csv(self.inputs.csv_file_path)
            logger.info(f"Successfully read CSV with {len(df)} rows")
            
            # Extract file paths from first column
            file_paths = df.iloc[:, 0].tolist()
            logger.info(f"Extracted {len(file_paths)} file paths")
            
            # Convert to JSON string
            file_paths_json = json.dumps(file_paths)
            
            logger.info("Successfully processed CSV input")
            print(f"Read {len(file_paths)} file paths from CSV")
            
            return self.Outputs(
                file_paths=file_paths_json
            )
            
        except Exception as e:
            logger.error(f"Failed to process CSV input: {e}")
            raise
