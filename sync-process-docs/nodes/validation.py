import json
import logging
from exospherehost import BaseNode
from pydantic import BaseModel

# Configure logger for this node
logger = logging.getLogger(__name__)


class ValidationNode(BaseNode):

    class Inputs(BaseModel):
        file_info: str  # JSON string with file processing information

    class Outputs(BaseModel):
        validated_result: str  # JSON string with validated result
        file_info: str  # JSON string with file information

    class Secrets(BaseModel):
        pass

    async def execute(self) -> Outputs:
        """
        Validate the extracted data from a single file processing result.
        """
        logger.info("Starting validation process")
        
        # Parse inputs
        try:
            file_info = json.loads(self.inputs.file_info)
            logger.info(f"Validating result for file: {file_info.get('file_path', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse file info: {e}")
            raise
        
        try:
            # Extract response content
            response_content = file_info.get("response_content", "")
            file_path = file_info.get("file_path", "")
            task_id = file_info.get("task_id", "")
            status = file_info.get("status", "unknown")
            
            if status != "completed":
                logger.warning(f"File {file_path} processing failed: {file_info.get('error', 'Unknown error')}")
                validated_result = {
                    "task_id": task_id,
                    "file_path": file_path,
                    "status": "failed",
                    "error": file_info.get("error", "Processing failed"),
                    "validation_timestamp": self._get_timestamp()
                }
            else:
                # Try to parse the response content as JSON
                try:
                    extracted_data = json.loads(response_content)
                    logger.info(f"Successfully parsed JSON response for file: {file_path}")
                except json.JSONDecodeError:
                    # If not JSON, treat as plain text
                    logger.warning(f"Response for file {file_path} is not valid JSON, treating as plain text")
                    extracted_data = {
                        "title": "Document",
                        "content": response_content,
                        "metadata": {
                            "file_path": file_path,
                            "task_id": task_id,
                            "response_format": "plain_text"
                        }
                    }
                
                # Create validated result
                validated_result = {
                    "task_id": task_id,
                    "file_path": file_path,
                    "status": "completed",
                    "extracted_data": extracted_data,
                    "usage_metadata": file_info.get("usage_metadata", {}),
                    "validation_timestamp": self._get_timestamp()
                }
                
                logger.info(f"Successfully validated result for file: {file_path}")
            
            return self.Outputs(
                validated_result=json.dumps(validated_result),
                file_info=self.inputs.file_info
            )
            
        except Exception as e:
            logger.error(f"Validation process failed: {e}")
            raise
    
    def _get_timestamp(self) -> str:
        """Get current timestamp as ISO string."""
        from datetime import datetime
        return datetime.now().isoformat()
