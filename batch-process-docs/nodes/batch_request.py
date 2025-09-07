import json
import logging
from exospherehost import BaseNode
from pydantic import BaseModel
from google import genai

# Configure logger for this node
logger = logging.getLogger(__name__)


class BatchRequestNode(BaseNode):

    class Inputs(BaseModel):
        parsed_files: str  # JSON string with parsed file contents from file_parsing node
        task_id: str  # Task ID from file_parsing node
        prompt: str  # Prompt for processing

    class Outputs(BaseModel):
        task_id: str  # Unique task ID for tracking
        batch_info: str  # JSON string with batch information

    class Secrets(BaseModel):
        gemini_api_key: str

    async def execute(self) -> Outputs:
        """
        Create a Gemini batch request from parsed file contents.
        """
        logger.info("Starting batch request creation")
        
        # Parse inputs
        try:
            parsed_files = json.loads(self.inputs.parsed_files)
            logger.info(f"Creating batch request for {len(parsed_files)} parsed files")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse parsed_files: {e}")
            raise
        
        # Create batch info
        batch_info = {
            "task_id": self.inputs.task_id,
            "file_count": len(parsed_files),
            "file_paths": [file_data["file_path"] for file_data in parsed_files],
            "status": "processing"
        }
        
        # Initialize Gemini client
        print(f"Gemini API key: {self.secrets.gemini_api_key}")
        client = genai.Client(api_key=self.secrets.gemini_api_key)
        
        logger.info(f"Submitting batch {self.inputs.task_id} to Gemini using Batch Mode")
        
        # Create inline requests for Gemini batch processing
        inline_requests = []
        for file_data in parsed_files:
            # Create inline request for this file
            request = {
                'contents': [{
                    'parts': [{
                        'text': f"{self.inputs.prompt}\n\nDocument content:\n{file_data['content']}"
                    }],
                    'role': 'user'
                }]
            }
            
            inline_requests.append(request)
                
        # Create batch job using Gemini API Batch Mode
        logger.info(f"Creating Gemini batch job with {len(inline_requests)} inline requests")
        
        inline_batch_job = client.batches.create(
            model="models/gemini-2.5-flash",
            src=inline_requests,
            config={
                'display_name': f"batch_job_{self.inputs.task_id}",
            },
        )
        
        # Get the actual batch ID from Gemini
        actual_batch_id = inline_batch_job.name
        logger.info(f"Successfully created Gemini batch: {actual_batch_id}")
        
        # Update batch info with actual batch ID
        batch_info["gemini_batch_id"] = actual_batch_id
        batch_info["request_count"] = len(inline_requests)
        batch_info["status"] = "submitted"
        
        print(f"Submitted batch {actual_batch_id} with {len(inline_requests)} requests for processing")
        
        return self.Outputs(
            task_id=actual_batch_id,  # Use actual Gemini batch ID
            batch_info=json.dumps(batch_info)
        )
