import logging
from dotenv import load_dotenv
from exospherehost import Runtime
from nodes.csv_input import CSVInputNode
from nodes.file_distribution import FileDistributionNode
from nodes.sync_processing import SyncProcessingNode
from nodes.validation import ValidationNode
from nodes.database_write import DatabaseWriteNode
from nodes.failure_handling import FailureHandlingNode
from logging_config import setup_logging

# Set up logging
setup_logging(level=logging.INFO, log_file="logs/app.log")

# Load environment variables from .env file
# EXOSPHERE_STATE_MANAGER_URI is the URI of the state manager
# EXOSPHERE_API_KEY is the key of the runtime
load_dotenv()

logger = logging.getLogger(__name__)
logger.info("Starting sync-process-docs runtime")

# Note on node ordering:
# The order of node classes in the `nodes` list does not define execution sequence.
# Nodes are registered with the state manager; orchestration and dependencies are handled externally.
# Nodes are listed in logical processing order for readability only.
Runtime(
    name="sync-process-docs-runtime",
    namespace="sync-process-docs",
    nodes=[
        CSVInputNode,
        FileDistributionNode,
        SyncProcessingNode,
        ValidationNode,
        DatabaseWriteNode,
        FailureHandlingNode
    ]
).start()
