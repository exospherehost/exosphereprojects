import logging
from dotenv import load_dotenv
from exospherehost import Runtime
from nodes.csv_input import CSVInputNode
from nodes.chunking import ChunkingNode
from nodes.batch_processing import BatchProcessingNode
from nodes.polling import PollingNode
from nodes.split_results import SplitResultsNode
from nodes.validation import ValidationNode
from nodes.database_write import DatabaseWriteNode
from nodes.failure_handling import FailureHandlingNode
from nodes.file_parsing import FileParsingNode
from nodes.batch_request import BatchRequestNode
from logging_config import setup_logging

# Set up logging
setup_logging(level=logging.INFO, log_file="logs/app.log")

# Load environment variables from .env file
# EXOSPHERE_STATE_MANAGER_URI is the URI of the state manager
# EXOSPHERE_API_KEY is the key of the runtime
load_dotenv()

logger = logging.getLogger(__name__)
logger.info("Starting batch-process-docs runtime")

# Note on node ordering:
# The order of node classes in the `nodes` list does not define execution sequence.
# Nodes are registered with the state manager; orchestration and dependencies are handled externally.
# Nodes are listed in logical processing order for readability only.
Runtime(
    name="batch-process-docs-runtime",
    namespace="batch-process-docs",
    nodes=[
        CSVInputNode,
        ChunkingNode,
        BatchProcessingNode,
        FileParsingNode,
        BatchRequestNode,
        PollingNode,
        SplitResultsNode,
        ValidationNode,
        DatabaseWriteNode,
        FailureHandlingNode
    ]
).start()
