import json
import logging
import csv
import os
from datetime import datetime
from exospherehost import BaseNode
from pydantic import BaseModel

# Configure logger for this node
logger = logging.getLogger(__name__)


class FailureHandlingNode(BaseNode):

    class Inputs(BaseModel):
        validation_status: str  # Status of validation (valid, invalid, partial)
        batch_info: str  # JSON string with batch information
        validated_data: str  # JSON string with validated data

    class Outputs(BaseModel):
        failure_csv_path: str  # Path to the failure CSV file
        retry_count: str  # Number of files that need retry

    class Secrets(BaseModel):
        pass

    async def execute(self) -> Outputs:
        """
        Handle validation failures for individual result and create retry CSV.
        """
        logger.info("Starting failure handling process for individual result")
        
        # Parse inputs
        try:
            batch_info = json.loads(self.inputs.batch_info)
            validated_data = json.loads(self.inputs.validated_data)
            logger.info(f"Handling failures for task {validated_data.get('task_id', 'unknown')}, file: {validated_data.get('file_path', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        # Check if there are any failures to handle
        if self.inputs.validation_status == "valid":
            logger.info("No failures to handle - validation passed")
            return self.Outputs(
                failure_csv_path="",
                retry_count="0"
            )
        
        try:
            # Create failure CSV for individual result
            failure_csv_path = await self._create_failure_csv(batch_info, validated_data)
            
            # For individual results, retry count is 1 if failed, 0 if valid
            retry_count = 1 if self.inputs.validation_status in ["invalid", "partial"] else 0
            
            logger.info(f"Failure handling completed: {retry_count} files need retry")
            print(f"Created failure CSV: {failure_csv_path}, Retry count: {retry_count}")
            
            return self.Outputs(
                failure_csv_path=failure_csv_path,
                retry_count=str(retry_count)
            )
            
        except Exception as e:
            logger.error(f"Failure handling process failed: {e}")
            raise
    
    async def _create_failure_csv(self, batch_info: dict, validated_data: dict) -> str:
        """Create a CSV file with failed file path for retry."""
        
        # Create failures directory if it doesn't exist
        failures_dir = "failures"
        os.makedirs(failures_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        task_id = validated_data.get("task_id", "unknown")
        filename = f"failures_{task_id}_{timestamp}.csv"
        file_path = os.path.join(failures_dir, filename)
        
        # Get the failed file path
        failed_file_path = validated_data.get("file_path", "")
        failure_reason = await self._get_failure_reason(validated_data)
        
        # Write failure CSV
        try:
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow(['file_path', 'failure_reason', 'task_id', 'timestamp'])
                
                # Write failed file
                writer.writerow([
                    failed_file_path,
                    failure_reason,
                    task_id,
                    datetime.now().isoformat()
                ])
            
            logger.info(f"Created failure CSV: {file_path} with 1 failed file")
            return file_path
            
        except Exception as e:
            logger.error(f"Failed to create failure CSV: {e}")
            raise
    
    async def _get_failure_reason(self, validated_data: dict) -> str:
        """Get the reason for failure for the individual result."""
        
        # Check for specific failure reasons based on validation status
        if self.inputs.validation_status == "invalid":
            return "schema_validation_failed"
        elif self.inputs.validation_status == "partial":
            extracted_data = validated_data.get("extracted_data", {})
            
            # Check for specific failure reasons
            if not extracted_data.get("title"):
                return "missing_title"
            elif not extracted_data.get("content"):
                return "missing_content"
            elif len(extracted_data.get("content", "").strip()) < 10:
                return "content_too_short"
            else:
                return "validation_failed"
        else:
            return "unknown_failure"
