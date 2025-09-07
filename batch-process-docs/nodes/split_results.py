import json
import logging
from exospherehost import BaseNode
from pydantic import BaseModel
from typing import List, Dict, Any

# Configure logger for this node
logger = logging.getLogger(__name__)


class SplitResultsNode(BaseNode):

    class Inputs(BaseModel):
        task_result: str  # JSON string with task results
        batch_info: str  # JSON string with batch information

    class Outputs(BaseModel):
        individual_result: str  # JSON string with individual result
        batch_info: str  # JSON string with batch information

    class Secrets(BaseModel):
        pass

    async def execute(self) -> List[Outputs]:
        """
        Split task results into individual states for parallel validation.
        Each result will be processed separately by the validation node.
        """
        logger.info("Starting result splitting process")
        
        # Parse inputs
        try:
            task_result = json.loads(self.inputs.task_result)
            batch_info = json.loads(self.inputs.batch_info)
            logger.info(f"Splitting results for task {batch_info.get('task_id', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        try:
            # Extract results from task_result
            results = task_result.get("results", [])
            task_id = task_result.get("task_id", "unknown")
            status = task_result.get("status", "unknown")
            
            if not results:
                logger.warning("No results found in task_result")
                return []
            
            # Create individual outputs for each result
            outputs = []
            for i, result in enumerate(results):
                
                # Extract the response content
                extracted_data = {}
                file_path = ""
                
                # Get the content from the result
                content = result.get("content", "")
                response_id = result.get("response_id", f"response_{i}")
                model_version = result.get("model_version", "unknown")
                usage_metadata = result.get("usage_metadata", {})
                
                if content:
                    try:
                        # Try to parse as JSON first
                        extracted_data = json.loads(content)
                    except json.JSONDecodeError:
                        # If not JSON, treat as plain text
                        extracted_data = {
                            "title": "Document",
                            "content": content,
                            "metadata": {
                                "response_id": response_id,
                                "model_version": model_version,
                                "usage_metadata": usage_metadata
                            }
                        }
                else:
                    # Handle empty content case
                    extracted_data = {
                        "title": "Empty Response",
                        "content": "No content received",
                        "metadata": {
                            "response_id": response_id,
                            "model_version": model_version,
                            "usage_metadata": usage_metadata
                        }
                    }
                
                # Try to extract file path from response_id if it follows a pattern
                # This might need adjustment based on how response_ids are generated
                if "_" in response_id:
                    # Assuming response_id might contain file info
                    file_path = response_id
                
                individual_result = {
                    "task_id": task_id,
                    "status": status,
                    "result_index": i,
                    "response_id": response_id,
                    "model_version": model_version,
                    "usage_metadata": usage_metadata,
                    "file_path": file_path,
                    "extracted_data": extracted_data,
                    "batch_info": batch_info,
                    "split_timestamp": self._get_timestamp()
                }
                
                outputs.append(self.Outputs(
                    individual_result=json.dumps(individual_result),
                    batch_info=json.dumps(batch_info)
                ))
            
            logger.info(f"Split {len(outputs)} results into individual states")
            print(f"Split {len(outputs)} results for parallel validation")
            
            return outputs
            
        except Exception as e:
            logger.error(f"Result splitting process failed: {e}")
            raise
    
    def _get_timestamp(self) -> str:
        """Get current timestamp as ISO string."""
        from datetime import datetime
        return datetime.now().isoformat()
