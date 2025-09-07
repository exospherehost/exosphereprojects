import json
import logging
import jsonschema
from exospherehost import BaseNode
from pydantic import BaseModel
from typing import Dict, Any

# Configure logger for this node
logger = logging.getLogger(__name__)


class ValidationNode(BaseNode):

    class Inputs(BaseModel):
        individual_result: str  # JSON string with individual result from split
        batch_info: str  # JSON string with batch information

    class Outputs(BaseModel):
        validated_data: str  # JSON string with validated data
        validation_status: str  # Status of validation (valid, invalid, partial)

    class Secrets(BaseModel):
        pass

    async def execute(self) -> Outputs:
        """
        Validate extracted JSON information from individual task result.
        """
        logger.info("Starting validation process for individual result")
        
        # Parse inputs
        try:
            individual_result = json.loads(self.inputs.individual_result)
            batch_info = json.loads(self.inputs.batch_info)
            logger.info(f"Validating individual result for task {individual_result.get('task_id', 'unknown')}, file: {individual_result.get('file_path', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        # Define the expected JSON schema for individual extracted data
        schema = {
            "type": "object",
            "properties": {
                "task_id": {"type": "string"},
                "status": {"type": "string"},
                "result_index": {"type": "integer"},
                "file_path": {"type": "string"},
                "extracted_data": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "content": {"type": "string"},
                        "metadata": {"type": "object"}
                    },
                    "required": ["title", "content"]
                },
                "batch_info": {"type": "object"},
                "split_timestamp": {"type": "string"}
            },
            "required": ["task_id", "status", "file_path", "extracted_data"]
        }
        
        try:
            # Validate the individual result against the schema
            jsonschema.validate(individual_result, schema)
            logger.info("Individual result validation successful")
            
            # Additional validation checks for the single result
            file_path = individual_result.get("file_path", "")
            extracted_data = individual_result.get("extracted_data", {})
            validation_status = "valid"
            
            # Check if required fields are present and not empty
            if not extracted_data.get("title") or not extracted_data.get("content"):
                logger.warning(f"Missing required fields for file: {file_path}")
                validation_status = "partial"
            
            # Check content quality (basic checks)
            content = extracted_data.get("content", "")
            if len(content.strip()) < 10:
                logger.warning(f"Content too short for file: {file_path}")
                validation_status = "partial"
            
            # Create validated data structure for individual result
            validated_data = {
                "task_id": individual_result.get("task_id"),
                "status": individual_result.get("status"),
                "result_index": individual_result.get("result_index"),
                "file_path": file_path,
                "extracted_data": extracted_data,
                "validation_timestamp": self._get_timestamp(),
                "validation_status": validation_status,
                "batch_info": batch_info
            }
            
            logger.info(f"Validation completed for file: {file_path}, status: {validation_status}")
            print(f"Validation status: {validation_status} for file: {file_path}")
            
            return self.Outputs(
                validated_data=json.dumps(validated_data),
                validation_status=validation_status
            )
            
        except jsonschema.ValidationError as e:
            logger.error(f"Schema validation failed: {e}")
            validation_status = "invalid"
            
            # Create error response for individual result
            validated_data = {
                "task_id": individual_result.get("task_id", "unknown"),
                "status": "validation_failed",
                "result_index": individual_result.get("result_index", -1),
                "file_path": individual_result.get("file_path", "unknown"),
                "error": str(e),
                "validation_timestamp": self._get_timestamp(),
                "validation_status": validation_status,
                "batch_info": batch_info
            }
            
            return self.Outputs(
                validated_data=json.dumps(validated_data),
                validation_status=validation_status
            )
            
        except Exception as e:
            logger.error(f"Validation process failed: {e}")
            raise
    
    def _get_timestamp(self) -> str:
        """Get current timestamp as ISO string."""
        from datetime import datetime
        return datetime.now().isoformat()
