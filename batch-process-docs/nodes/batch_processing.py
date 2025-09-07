import json
import logging
import uuid
from exospherehost import BaseNode
from pydantic import BaseModel
from google import genai
from google.genai import types

# Configure logger for this node
logger = logging.getLogger(__name__)


class BatchProcessingNode(BaseNode):

    class Inputs(BaseModel):
        chunk: str  # JSON string representation of chunk of file paths
        prompt: str  # Prompt for OpenAI processing

    class Outputs(BaseModel):
        task_id: str  # Unique task ID for tracking
        batch_info: str  # JSON string with batch information

    class Secrets(BaseModel):
        gemini_api_key: str

    async def execute(self) -> Outputs:
        """
        Process a batch of file paths by sending to Gemini batch endpoint.
        """
        logger.info("Starting batch processing with Gemini")
        
        # Parse inputs
        try:
            file_paths = json.loads(self.inputs.chunk)
            logger.info(f"Processing batch with {len(file_paths)} files")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse chunk: {e}")
            raise
        
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # Create batch info
        batch_info = {
            "task_id": task_id,
            "file_count": len(file_paths),
            "file_paths": file_paths,
            "prompt": self.inputs.prompt,
            "status": "processing"
        }
        
        # Initialize Gemini client
        client = genai.Client(api_key=self.secrets.gemini_api_key)
        
        logger.info(f"Submitting batch {task_id} to Gemini using Batch Mode")
        
        # Read files and prepare inline requests for Gemini batch processing
        inline_requests = []
        for file_path in file_paths:
            
            # Read file content based on file type
            if file_path.endswith('.txt'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            elif file_path.endswith('.pdf'):
                # Read PDF files using pdfplumber
                try:
                    import pdfplumber
                    with pdfplumber.open(file_path) as pdf:
                        content = ""
                        for page in pdf.pages:
                            page_text = page.extract_text()
                            if page_text:
                                content += page_text + "\n"
                except ImportError:
                    logger.warning("pdfplumber not available, using PyPDF2")
                    import PyPDF2
                    with open(file_path, 'rb') as f:
                        pdf_reader = PyPDF2.PdfReader(f)
                        content = ""
                        for page in pdf_reader.pages:
                            content += page.extract_text() + "\n"
            elif file_path.endswith('.docx'):
                # Read DOCX files using python-docx
                try:
                    from docx import Document
                    doc = Document(file_path)
                    content = ""
                    for paragraph in doc.paragraphs:
                        content += paragraph.text + "\n"
                except ImportError:
                    logger.error("python-docx not available for DOCX processing")
                    content = f"[DOCX content from {file_path} - python-docx not installed]"
            else:
                # Try to read as text file
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            
            # Create inline request for this file
            request = {
                'contents': [{
                    'parts': [{
                        'text': f"{self.inputs.prompt}\n\nDocument content:\n{content}"
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
                'display_name': f"batch_job_{task_id}",
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
        
