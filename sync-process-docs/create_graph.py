#!/usr/bin/env python3
"""
Sync Document Processing Graph Template

This file defines the graph template for the sync document processing workflow.
It takes a CSV file with document paths, processes them one at a time using real-time API, and stores extracted information in a database.
"""

import asyncio
import os
from exospherehost import StateManager, GraphNodeModel, RetryPolicyModel, StoreConfigModel, RetryStrategyEnum

from dotenv import load_dotenv

load_dotenv()

# Environment variables
EXOSPHERE_STATE_MANAGER_URI = os.getenv("EXOSPHERE_STATE_MANAGER_URI", "http://localhost:8000")
EXOSPHERE_API_KEY = os.getenv("EXOSPHERE_API_KEY", "exosphere@123")  # TODO: Replace with your actual API key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "{{GEMINI_API_KEY}}")
DATABASE_URL = os.getenv("MONGODB_CONNECTION_STRING", "{{DATABASE_URL}}")
DATABASE_NAME = os.getenv("DATABASE_NAME", "sync_processed_docs")

async def create_graph():
    """Create a graph with sync document processing nodes using Exosphere Python SDK"""
    state_manager = StateManager(
        namespace="sync-process-docs",
        state_manager_uri=EXOSPHERE_STATE_MANAGER_URI,
        key=EXOSPHERE_API_KEY
    )

    graph_nodes = [
        GraphNodeModel(
            node_name="CSVInputNode",
            namespace="sync-process-docs",
            identifier="csv_input",
            inputs={
                "csv_file_path": "${{ store.csv_file_path }}"
            },
            next_nodes=["file_distribution"]
        ),
        GraphNodeModel(
            node_name="FileDistributionNode",
            namespace="sync-process-docs",
            identifier="file_distribution",
            inputs={
                "file_paths": "${{ csv_input.outputs.file_paths }}"
            },
            next_nodes=["sync_processing"]
        ),
        GraphNodeModel(
            node_name="SyncProcessingNode",
            namespace="sync-process-docs",
            identifier="sync_processing",
            inputs={
                "file_path": "${{ file_distribution.outputs.file_path }}",
                "prompt": "${{ store.prompt }}"
            },
            next_nodes=["validation"]
        ),
        GraphNodeModel(
            node_name="ValidationNode",
            namespace="sync-process-docs",
            identifier="validation",
            inputs={
                "file_info": "${{ sync_processing.outputs.file_info }}"
            },
            next_nodes=["database_write", "failure_handling"]
        ),
        GraphNodeModel(
            node_name="DatabaseWriteNode",
            namespace="sync-process-docs",
            identifier="database_write",
            inputs={
                "validated_result": "${{ validation.outputs.validated_result }}",
                "file_info": "${{ validation.outputs.file_info }}"
            },
            next_nodes=[]
        ),
        GraphNodeModel(
            node_name="FailureHandlingNode",
            namespace="sync-process-docs",
            identifier="failure_handling",
            inputs={
                "validated_result": "${{ validation.outputs.validated_result }}",
                "file_info": "${{ validation.outputs.file_info }}"
            },
            next_nodes=[]
        )
    ]

    retry_policy = RetryPolicyModel(
        max_retries=3,
        strategy=RetryStrategyEnum.EXPONENTIAL,
        backoff_factor=2000,
        exponent=2
    )

    store_config = StoreConfigModel(
        required_keys=["csv_file_path", "prompt"],
        default_values={       
            "csv_file_path": "",
            "prompt": "Extract key information from this document"
        }
    )

    result = await state_manager.upsert_graph(
        graph_name="parse-and-process-insync-docs",
        graph_nodes=graph_nodes,
        secrets={
            "gemini_api_key": GEMINI_API_KEY,
            "mongodb_connection_string": DATABASE_URL,
            "database_name": DATABASE_NAME
        },
        retry_policy=retry_policy,
        store_config=store_config
    )
    return result

async def main():
    """Main function to create the sync document processing graph"""
    try:
        result = await create_graph()
        if result:
            print("Sync document processing graph created successfully!")
            print(f"Graph result: {result}")
        else:
            print("Failed to create graph")
    except Exception as e:
        print(f"Error creating graph: {e}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
