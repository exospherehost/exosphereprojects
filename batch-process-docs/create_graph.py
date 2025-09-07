#!/usr/bin/env python3
"""
Batch Document Processing Graph Template

This file defines the graph template for the batch document processing workflow.
It takes a CSV file with document paths, processes them in batches, and stores extracted information in a database.
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
DATABASE_URL = os.getenv("DATABASE_URL", "{{DATABASE_URL}}")

async def create_graph():
    """Create a graph with batch document processing nodes using Exosphere Python SDK"""
    state_manager = StateManager(
        namespace="batch-process-docs",
        state_manager_uri=EXOSPHERE_STATE_MANAGER_URI,
        key=EXOSPHERE_API_KEY
    )

    graph_nodes = [
        GraphNodeModel(
            node_name="CSVInputNode",
            namespace="batch-process-docs",
            identifier="csv_input",
            inputs={
                "csv_file_path": "${{ store.csv_file_path }}"
            },
            next_nodes=["chunking"]
        ),
        GraphNodeModel(
            node_name="ChunkingNode",
            namespace="batch-process-docs",
            identifier="chunking",
            inputs={
                "file_paths": "${{ csv_input.outputs.file_paths }}",
                "chunk_size": "${{ store.chunk_size }}"
            },
            next_nodes=["file_parsing"]
        ),
        GraphNodeModel(
            node_name="FileParsingNode",
            namespace="batch-process-docs",
            identifier="file_parsing",
            inputs={
                "chunk": "${{ chunking.outputs.chunk }}"
            },
            next_nodes=["batch_request"]
        ),
        GraphNodeModel(
            node_name="BatchRequestNode",
            namespace="batch-process-docs",
            identifier="batch_request",
            inputs={
                "parsed_files": "${{ file_parsing.outputs.parsed_files }}",
                "task_id": "${{ file_parsing.outputs.task_id }}",
                "prompt": "${{ store.prompt }}"
            },
            next_nodes=["polling"]
        ),
        GraphNodeModel(
            node_name="PollingNode",
            namespace="batch-process-docs",
            identifier="polling",
            inputs={
                "task_id": "${{ batch_request.outputs.task_id }}",
                "batch_info": "${{ batch_request.outputs.batch_info }}"
            },
            next_nodes=["split_results"]
        ),
        GraphNodeModel(
            node_name="SplitResultsNode",
            namespace="batch-process-docs",
            identifier="split_results",
            inputs={
                "task_result": "${{ polling.outputs.task_result }}",
                "batch_info": "${{ polling.outputs.batch_info }}"
            },
            next_nodes=["validation"]
        ),
        GraphNodeModel(
            node_name="ValidationNode",
            namespace="batch-process-docs",
            identifier="validation",
            inputs={
                "individual_result": "${{ split_results.outputs.individual_result }}",
                "batch_info": "${{ split_results.outputs.batch_info }}"
            },
            next_nodes=["database_write", "failure_handling"]
        ),
        GraphNodeModel(
            node_name="DatabaseWriteNode",
            namespace="batch-process-docs",
            identifier="database_write",
            inputs={
                "validated_data": "${{ validation.outputs.validated_data }}",
                "batch_info": "${{ split_results.outputs.batch_info }}"
            },
            next_nodes=[]
        ),
        GraphNodeModel(
            node_name="FailureHandlingNode",
            namespace="batch-process-docs",
            identifier="failure_handling",
            inputs={
                "validation_status": "${{ validation.outputs.validation_status }}",
                "batch_info": "${{ split_results.outputs.batch_info }}",
                "validated_data": "${{ validation.outputs.validated_data }}"
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
        required_keys=["csv_file_path", "chunk_size", "prompt"],
        default_values={       
            "csv_file_path": "",
            "chunk_size": "5",
            "prompt": "Extract key information from this document"
        }
    )

    result = await state_manager.upsert_graph(
        graph_name="parse-and-process-batch-docs1",
        graph_nodes=graph_nodes,
        secrets={
            "gemini_api_key": GEMINI_API_KEY,
            "database_url": DATABASE_URL
        },
        retry_policy=retry_policy,
        store_config=store_config
    )
    return result

async def main():
    """Main function to create the batch document processing graph"""
    try:
        result = await create_graph()
        if result:
            print("Batch document processing graph created successfully!")
            print(f"Graph result: {result}")
        else:
            print("Failed to create graph")
    except Exception as e:
        print(f"Error creating graph: {e}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
