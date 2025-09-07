import json
import logging
from exospherehost import BaseNode
from pydantic import BaseModel
from typing import List, Dict, Any

# Configure logger for this node
logger = logging.getLogger(__name__)


class FileParsingNode(BaseNode):

    class Inputs(BaseModel):
        chunk: str  # JSON string representation of chunk of file paths

    class Outputs(BaseModel):
        parsed_files: str  # JSON string with parsed file contents
        task_id: str  # Unique task ID for tracking

    async def execute(self) -> Outputs:
        """
        Parse files from the provided file paths and extract their content.
        """
        logger.info("Starting file parsing")
        
        # Parse inputs
        try:
            file_paths = json.loads(self.inputs.chunk)
            logger.info(f"Parsing {len(file_paths)} files")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse chunk: {e}")
            raise
        
        # Generate unique task ID
        import uuid
        task_id = str(uuid.uuid4())
        
        # Parse files and extract content
        parsed_files = []
        for file_path in file_paths:
            try:
                content = self._read_file_content(file_path)
                parsed_files.append({
                    "file_path": file_path,
                    "content": content
                })
                logger.debug(f"Successfully parsed file: {file_path}")
            except Exception as e:
                logger.error(f"Failed to parse file {file_path}: {e}")
                # Add error information but continue processing other files
                parsed_files.append({
                    "file_path": file_path,
                    "content": f"[ERROR: Failed to read file - {str(e)}]",
                    "error": str(e)
                })
        
        logger.info(f"Successfully parsed {len(parsed_files)} files")
        
        return self.Outputs(
            task_id=task_id,
            parsed_files=json.dumps(parsed_files)
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
