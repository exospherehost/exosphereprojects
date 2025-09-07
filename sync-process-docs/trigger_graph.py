#!/usr/bin/env python3
"""
Sync Document Processing Graph Trigger

This script triggers the sync document processing workflow
with a real CSV file containing document paths and processing parameters using the Exosphere Python SDK.
"""

import asyncio
import os
import sys
import csv
from exospherehost import StateManager
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def trigger_graph_execution(graph_name: str="sync-process-docs", csv_file_path: str="documents.csv", processing_prompt: str = None, gemini_api_key: str = None, database_url: str = None):
    """Trigger the sync document processing workflow using Exosphere Python SDK"""
    # Configuration from environment variables
    namespace = os.getenv("EXOSPHERE_NAMESPACE", "sync-process-docs")
    state_manager_uri = os.getenv("EXOSPHERE_STATE_MANAGER_URI", "http://localhost:8000")
    api_key = os.getenv("EXOSPHERE_API_KEY", "")  
    
    database_url = os.getenv("DATABASE_URL", "{{DATABASE_URL}}")
    gemini_api_key = os.getenv("GEMINI_API_KEY", "{{GEMINI_API_KEY}}")

    
    # Validate CSV file exists
    if not os.path.exists(csv_file_path):
        print(f"Error: CSV file '{csv_file_path}' does not exist")
        return None

    
    # Default processing prompt if not provided
    if not processing_prompt:
        processing_prompt = """
        Please extract the following information from each document:
        1. Document title
        2. Main content/summary
        3. Key metadata (page count, word count, etc.)
        4. Any important dates or numbers mentioned
        
        Return the information in JSON format with the following structure:
        {
            "title": "Document title",
            "content": "Main content or summary",
            "metadata": {
                "pages": number,
                "word_count": number,
                "dates": ["date1", "date2"],
                "numbers": ["number1", "number2"]
            }
        }
        """
    
    # Initialize state manager
    state_manager = StateManager(
        namespace=namespace,
        state_manager_uri=state_manager_uri,
        key=api_key
    )
    

    print("Triggering sync document processing workflow...")
    print(f"CSV file: {csv_file_path}")
    print("-" * 50)
    
    # Trigger the graph with store values (no inputs needed since parameters are in store)
    result = await state_manager.trigger(
        graph_name,
        store={
            "csv_file_path": csv_file_path,
            "prompt": processing_prompt
        }
    )
    
    
    return result['run_id']
    

async def main():
    graph_name = "parse-and-process-insync-docs"
    
    csv_file_path = "documents.csv"
    processing_prompt = "Extract title, content, and metadata from each document"
    gemini_api_key = os.getenv("GEMINI_API_KEY", "{{GEMINI_API_KEY}}")
    database_url = os.getenv("DATABASE_URL", "{{DATABASE_URL}}")
    
    run_id = await trigger_graph_execution(graph_name=graph_name, csv_file_path=csv_file_path, processing_prompt=processing_prompt, gemini_api_key=gemini_api_key, database_url=database_url)
    
    if run_id:
        print(f"\n   Run ID: {run_id}")
        print(f"   Graph: {graph_name}")
        print(f"\nYou can monitor the execution on the Exosphere dashboard.")
    else:
        print("Failed to trigger sync document processing workflow.")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
