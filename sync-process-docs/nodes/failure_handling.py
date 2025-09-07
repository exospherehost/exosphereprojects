import json
import logging
import os
import csv
from datetime import datetime
from exospherehost import BaseNode
from pydantic import BaseModel

# Configure logger for this node
logger = logging.getLogger(__name__)


class FailureHandlingNode(BaseNode):

    class Inputs(BaseModel):
        validated_result: str  # JSON string with validated result
        file_info: str  # JSON string with file information

    class Outputs(BaseModel):
        failure_status: str  # Status of failure handling

    class Secrets(BaseModel):
        pass

    async def execute(self) -> Outputs:
        """
        Handle failed document processing by creating retry files.
        """
        logger.info("Starting failure handling process")
        
        # Parse inputs
        try:
            validated_result = json.loads(self.inputs.validated_result)
            file_info = json.loads(self.inputs.file_info)
            logger.info(f"Checking failure status for file: {validated_result.get('file_path', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        status = validated_result.get("status", "unknown")
        file_path = validated_result.get("file_path", "")
        task_id = validated_result.get("task_id", "")
        
        if status == "failed":
            logger.warning(f"Handling failure for file: {file_path}")
            
            # Create failures directory if it doesn't exist
            failures_dir = "failures"
            if not os.path.exists(failures_dir):
                os.makedirs(failures_dir)
            
            # Create failure CSV file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            failure_csv = os.path.join(failures_dir, f"failed_files_{timestamp}.csv")
            
            # Write failure information to CSV
            with open(failure_csv, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['file_path', 'task_id', 'error', 'timestamp'])
                writer.writerow([
                    file_path,
                    task_id,
                    validated_result.get("error", "Unknown error"),
                    datetime.now().isoformat()
                ])
            
            logger.info(f"Created failure CSV: {failure_csv}")
            print(f"Created failure CSV for file: {file_path}")
            
            return self.Outputs(
                failure_status="failure_logged"
            )
        else:
            logger.info(f"File {file_path} processed successfully, no failure handling needed")
            return self.Outputs(
                failure_status="no_failure"
            )

