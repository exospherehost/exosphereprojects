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
        validated_data: str  # JSON string with validated data
        batch_info: str  # JSON string with batch information

    class Outputs(BaseModel):
        write_status: str  # Status of database write operation
        record_count: str  # Number of records written

    class Secrets(BaseModel):
        database_url: str        

    async def execute(self) -> Outputs:
        """
        Write individual validated result to database.
        """

        database_name: str = "processed_documents_db"
        collection_name: str = "processed_documents"

        logger.info("Starting database write operation for individual result")
        
        # Parse inputs
        try:
            validated_data = json.loads(self.inputs.validated_data)
            batch_info = json.loads(self.inputs.batch_info)
            logger.info(f"Writing individual result for task {validated_data.get('task_id', 'unknown')}, file: {validated_data.get('file_path', 'unknown')}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse inputs: {e}")
            raise
        
        try:
            # Create MongoDB client
            client = AsyncIOMotorClient(self.secrets.database_url)
            db = client[database_name]
            collection = db[collection_name]
            
            # Ensure collection exists and create indexes
            await self._ensure_collection_exists(collection)
            
            # Write individual validated result to database
            record_count = 0
            write_status = "success"
            
            try:
                await self._write_record(collection, validated_data, batch_info)
                record_count = 1
                logger.info(f"Successfully wrote record for file: {validated_data.get('file_path', 'unknown')}")
            except Exception as e:
                logger.error(f"Failed to write record for file {validated_data.get('file_path', 'unknown')}: {e}")
                write_status = "failed"
            finally:
                # Close the MongoDB client
                client.close()
            
            logger.info(f"Database write completed: {record_count} records written")
            print(f"Database write status: {write_status}, Records written: {record_count}")
            
            return self.Outputs(
                write_status=write_status,
                record_count=str(record_count)
            )
            
        except Exception as e:
            logger.error(f"Database write operation failed: {e}")
            raise
    
    async def _ensure_collection_exists(self, collection):
        """Ensure the collection exists and create indexes."""
        try:
            # Create indexes for better query performance
            await collection.create_index("task_id")
            await collection.create_index("file_path")
            await collection.create_index("processing_status")
            await collection.create_index("created_at")
            logger.info("Ensured collection exists with proper indexes")
        except PyMongoError as e:
            logger.error(f"Failed to create collection indexes: {e}")
            raise
    
    async def _write_record(self, collection, validated_data: dict, batch_info: dict):
        """Write a single record to the MongoDB collection."""
        current_time = datetime.now()
        
        # Create document for MongoDB
        document = {
            "task_id": validated_data.get("task_id", "unknown"),
            "file_path": validated_data.get("file_path", ""),
            "extracted_data": validated_data.get("extracted_data", {}),
            "processing_status": validated_data.get("validation_status", "completed"),
            "created_at": current_time,
            "updated_at": current_time,
            "batch_info": batch_info
        }
        
        try:
            result = await collection.insert_one(document)
            logger.info(f"Inserted document with ID: {result.inserted_id}")
        except PyMongoError as e:
            logger.error(f"Failed to insert document: {e}")
            raise
