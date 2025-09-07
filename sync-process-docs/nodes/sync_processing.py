import json
import logging
import uuid
from exospherehost import BaseNode
from pydantic import BaseModel
from google import genai

# Configure logger for this node
logger = logging.getLogger(__name__)


class SyncProcessingNode(BaseNode):

    class Inputs(BaseModel):
        file_path: str  # JSON string representation of a single file path
        prompt: str  # Prompt for Gemini processing

    class Outputs(BaseModel):
        task_id: str  # Unique task ID for tracking
        file_info: str  # JSON string with file information

    class Secrets(BaseModel):
        gemini_api_key: str

    async def execute(self) -> Outputs:
        """
        Process a single file by sending to Gemini real-time API.
        """
        logger.info("Starting sync processing with Gemini")
        
        # Parse inputs
        try:
            file_path = json.loads(self.inputs.file_path)
            logger.info(f"Processing file: {file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse file path: {e}")
            raise
        
        # Generate unique task ID
        task_id = str(uuid.uuid4())
        
        # Create file info
        file_info = {
            "task_id": task_id,
            "file_path": file_path,
            "prompt": self.inputs.prompt,
            "status": "processing"
        }
        
        # Initialize Gemini client
        client = genai.Client(api_key=self.secrets.gemini_api_key)
        
        logger.info(f"Submitting file {file_path} to Gemini using real-time API")
        
     
        # Read file content based on file type
        content = self._read_file_content(file_path)
        
        # Create the request for Gemini real-time API
        request = {
            'contents': [{
                'parts': [{
                    'text': f"{self.inputs.prompt}\n\nDocument content:\n{content}"
                }],
                'role': 'user'
            }]
        }
        
        # Send request to Gemini real-time API
        logger.info(f"Sending real-time request for file: {file_path}")
        response = client.models.generate_content(
            model="models/gemini-2.5-flash",
            contents=request['contents']
        )
        
        # Extract response content
        if response.candidates and len(response.candidates) > 0:
            response_content = response.candidates[0].content.parts[0].text
            usage_metadata = response.usage_metadata
            
            # Update file info with response
            file_info["status"] = "completed"
            file_info["response_content"] = response_content
            file_info["usage_metadata"] = {
                'prompt_token_count': usage_metadata.prompt_token_count,
                'candidates_token_count': usage_metadata.candidates_token_count,
                'total_token_count': usage_metadata.total_token_count,
                'cached_content_token_count': getattr(usage_metadata, 'cached_content_token_count', 0)
            }
            
            logger.info(f"Successfully processed file {file_path}")
            print(f"Processed file {file_path} with {usage_metadata.total_token_count} tokens")
            
        else:
            logger.error(f"No response received for file {file_path}")
            file_info["status"] = "failed"
            file_info["error"] = "No response received from Gemini"
            
    
        
        return self.Outputs(
            task_id=task_id,
            file_info=json.dumps(file_info)
        )
    
    def _read_file_content(self, file_path: str) -> str:
        """
        Read content from a file based on its extension.
        
        Args:
            file_path: Path to the file to read
            
        Returns:
            String content of the file
        """
        if file_path.endswith('.txt'):
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
                
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
                    return content
            except ImportError:
                logger.warning("pdfplumber not available, using PyPDF2")
                import PyPDF2
                with open(file_path, 'rb') as f:
                    pdf_reader = PyPDF2.PdfReader(f)
                    content = ""
                    for page in pdf_reader.pages:
                        content += page.extract_text() + "\n"
                    return content
                    
        elif file_path.endswith('.docx'):
            # Read DOCX files using python-docx
            try:
                from docx import Document
                doc = Document(file_path)
                content = ""
                for paragraph in doc.paragraphs:
                    content += paragraph.text + "\n"
                return content
            except ImportError:
                logger.error("python-docx not available for DOCX processing")
                return f"[DOCX content from {file_path} - python-docx not installed]"
                
        else:
            # Try to read as text file
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
