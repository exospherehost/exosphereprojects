import json
import logging
from datetime import datetime
from exospherehost import BaseNode
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError

# Configure logger for this node
logger = logging.getLogger(__name__)


class DatabaseWriteNode(BaseNode):

    class Inputs(BaseModel):
        validated_result: str  # JSON string with validated result
        file_info: str  # JSON string with file information

    class Outputs(BaseModel):
        write_status: str  # Status of database write operation

    class Secrets(BaseModel):
        mongodb_connection_string: str
        database_name: str = "sync_processed_docs"

    async def execute(self) -> Outputs:
        """
        Write validated result to database.
        """
        logger.info("Starting database write operation")
        
        # Parse inputs
        try:
            validated_result = json.loads(self.inputs.validated_result)
            file_info = json.loads(self.inputs.file_info)
            logger.info(f"Writing result for file: {validated_result.get('file_path', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        
        # Create MongoDB client
        client = AsyncIOMotorClient(self.secrets.mongodb_connection_string)
        db = client[self.secrets.database_name]
        collection = db.sync_processed_documents
        
        # Prepare data for insertion
        file_path = validated_result.get("file_path", "")
        task_id = validated_result.get("task_id", "")
        status = validated_result.get("status", "unknown")
        extracted_data = validated_result.get("extracted_data", {})
        usage_metadata = validated_result.get("usage_metadata", {})
        validation_timestamp = validated_result.get("validation_timestamp", "")
        
        # Create document for MongoDB
        document = {
            "task_id": task_id,
            "file_path": file_path,
            "status": status,
            "extracted_data": extracted_data,  # MongoDB stores JSON natively
            "usage_metadata": usage_metadata,  # MongoDB stores JSON natively
            "validation_timestamp": validation_timestamp,
            "updated_at": datetime.utcnow()
        }
        
        # Perform upsert operation (insert or update)
        result = await collection.update_one(
            {"task_id": task_id},  # Filter by task_id
            {
                "$set": document,
                "$setOnInsert": {"created_at": datetime.utcnow()}  # Only set on insert
            },
            upsert=True  # Insert if not found, update if found
        )
        
        # Close the client connection
        client.close()
            
        logger.info(f"Successfully wrote result to MongoDB for file: {file_path}")
        print(f"Wrote result to MongoDB for file: {file_path}")
        
        return self.Outputs(
            write_status="success"
        )

