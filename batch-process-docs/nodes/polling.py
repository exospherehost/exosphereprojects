import json
import logging
from datetime import timedelta
from exospherehost import BaseNode, ReQueueAfterSignal
from pydantic import BaseModel
from google import genai

# Configure logger for this node
logger = logging.getLogger(__name__)


class PollingNode(BaseNode):

    class Inputs(BaseModel):
        task_id: str  # Task ID to poll for
        batch_info: str  # JSON string with batch information

    class Outputs(BaseModel):
        task_result: str  # JSON string with task results
        batch_info: str  # JSON string with batch information

    class Secrets(BaseModel):
        gemini_api_key: str

    async def execute(self) -> Outputs:
        """
        Poll for task completion using ReQueueAfterSignal for requeuing.
        """
        logger.info(f"Polling task: {self.inputs.task_id}")
        
        # Parse batch info
        try:
            batch_info = json.loads(self.inputs.batch_info)
            logger.info(f"Polling task {self.inputs.task_id} with {batch_info.get('file_count', 0)} files")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse batch info: {e}")
            raise
        
        try:
            # Initialize Gemini client
            client = genai.Client(api_key=self.secrets.gemini_api_key)
            
            # Check task status
            task_status, batch_job = await self._check_task_status(client, self.inputs.task_id)
            logger.info(f"Task {self.inputs.task_id} status: {task_status}")
            
            if task_status == "completed":
                logger.info(f"Task {self.inputs.task_id} completed successfully")
                
                # Get the results
                task_result = await self._get_task_results(client, self.inputs.task_id, batch_job)
                
                return self.Outputs(
                    task_result=json.dumps(task_result),
                    batch_info=self.inputs.batch_info
                )
            
            elif task_status == "failed":
                logger.error(f"Task {self.inputs.task_id} failed")
                
                return self.Outputs(
                    task_result=json.dumps({"error": "Task failed"}),
                    batch_info=self.inputs.batch_info
                )
            
            else:  # pending, processing, or other non-terminal status
                logger.info(f"Task {self.inputs.task_id} still {task_status}, requeuing...")
                
                # Raise ReQueueAfterSignal to requeue the state after 30 seconds
                # The state manager will handle the requeuing automatically
                raise ReQueueAfterSignal(timedelta(seconds=30))
            
        except ReQueueAfterSignal:
            # Re-raise the signal to let the runtime handle it
            raise
        except Exception as e:
            logger.error(f"Failed to poll task {self.inputs.task_id}: {e}")
            raise
    
    async def _check_task_status(self, client, task_id: str) -> tuple:
        """
        Check the status of a task using Gemini's batch API.
        """
        try:
            # Call Gemini's batch status endpoint
            batch_job = client.batches.get(name=task_id)
            
            # Map Gemini batch statuses to our internal statuses
            status_mapping = {
                "JOB_STATE_PENDING": "pending",
                "JOB_STATE_RUNNING": "processing",
                "JOB_STATE_SUCCEEDED": "completed",
                "JOB_STATE_FAILED": "failed",
                "JOB_STATE_CANCELLING": "processing",
                "JOB_STATE_CANCELLED": "failed"
            }
            
            mapped_status = status_mapping.get(batch_job.state.name, "pending")
            logger.info(f"Batch {task_id} status: {batch_job.state.name} -> {mapped_status}")
            
            return (mapped_status, batch_job)
            
        except Exception as e:
            logger.error(f"Failed to check task status for {task_id}: {e}")
            return ("failed", None)
    
    async def _get_task_results(self, client, task_id: str, batch_job) -> dict:
        """
        Get the results of a completed task using Gemini's batch API.
        """
       
        # First, verify the batch is completed
        if batch_job.state.name != "JOB_STATE_SUCCEEDED":
            logger.warning(f"Batch {task_id} is not completed yet, status: {batch_job.state.name}")
            return {
                "task_id": task_id,
                "status": batch_job.state.name,
                "results": []
            }
        
        results = []
        
        # Check if there are inlined responses (new format)
        if hasattr(batch_job, 'dest') and hasattr(batch_job.dest, 'inlined_responses'):
            logger.info(f"Found {len(batch_job.dest.inlined_responses)} inlined responses for batch {task_id}")
            
            for i, inlined_response in enumerate(batch_job.dest.inlined_responses):
                response = inlined_response.response
                content = response.candidates[0].content.parts[0].text if response.candidates else "No content"
                usage = response.usage_metadata
                
                result = {
                    'response_id': response.response_id,
                    'model_version': response.model_version,
                    'content': content,
                    'usage_metadata': {
                        'prompt_token_count': usage.prompt_token_count,
                        'candidates_token_count': usage.candidates_token_count,
                        'total_token_count': usage.total_token_count,
                        'cached_content_token_count': getattr(usage, 'cached_content_token_count', 0)
                    }
                }
                results.append(result)
                logger.debug(f"Parsed inlined response {i+1}: {response.response_id}")
        
        # Fallback: Check if there's an output file (legacy format)
        elif hasattr(batch_job, 'dest') and hasattr(batch_job.dest, 'file_name') and batch_job.dest.file_name:
            logger.info(f"Downloading results from file: {batch_job.dest.file_name}")
            
            # Download and parse the results
            file_content_bytes = client.files.download(name=batch_job.dest.file_name)
            results_data = file_content_bytes.decode('utf-8')
            
            # Parse JSONL results
            for line in results_data.strip().split('\n'):
                if line.strip():
                    result = json.loads(line)
                    results.append(result)
        
        else:
            logger.error(f"No inlined responses or output file found for completed batch {task_id}")
            return {
                "task_id": task_id,
                "status": "failed",
                "error": "No output file or inlined responses available",
                "results": []
            }
        
        logger.info(f"Retrieved {len(results)} results from batch {task_id}")
        
        return {
            "task_id": task_id,
            "status": "completed",
            "results": results
        }
