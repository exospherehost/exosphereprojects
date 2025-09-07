# Batch Document Processing - Getting Started Guide

## What is this project?

This is a complete batch document processing system that can extract information from large numbers of documents using AI. It's designed to handle hundreds or thousands of documents efficiently by processing them in batches, using Gemini's API to extract structured data, and storing the results in a database.

**Perfect for:**
- Processing large document collections
- Extracting structured data from unstructured documents
- Automating document analysis workflows
- Converting documents to searchable, structured formats

## Quick Start

### Prerequisites
- Python 3.8 or higher
- Gemini API key
- MongoDB URI
- Exosphere account and API key

### 1. Clone and Setup
```bash
# Navigate to the project directory
cd batch-process-docs

# Copy the environment template
cp env.example .env

# Edit .env with your credentials
# You'll need: EXOSPHERE_API_KEY, GEMINI_API_KEY, DATABASE_URL
```

Setup env
```bash
uv init
```

### 2. Register with Exosphere
```bash
# Register your runtime with Exosphere
uv run register.py
```

### 3. Create Workflow Template
```bash
# Create the processing workflow template
uv run create_graph.py
```

### 4. Test with Sample Data
```bash
# Process sample documents
uv run trigger_graph.py
```

## How It Works

The system processes documents in 7 main steps:

1. **CSV Input**: Reads a list of document file paths from a CSV file
2. **Chunking**: Groups documents into batches (e.g., 10 documents per batch)
3. **AI Processing**: Sends each batch to Gemini for information extraction
4. **Polling**: Waits for Gemini to complete processing
5. **Validation**: Checks that the extracted data is valid JSON
6. **Database Storage**: Saves the results to your database
7. **Error Handling**: Creates retry files for any failed documents

## Your First Document Processing Job

### Step 1: Prepare Your Documents
Create a CSV file with your document paths:
```csv
file_path
/path/to/document1.pdf
/path/to/document2.docx
/path/to/document3.txt
```

### Step 2: Configure Your Processing
Edit the processing prompt in `processing_prompt_template.txt` to tell the AI what information to extract from your documents.

### Step 3: Run the Processing
```bash
# Process your documents
python trigger_graph.py
```

### Step 4: Check Results
- View processed data in your database
- Check `logs/app.log` for processing details
- Look in `failures/` folder for any documents that need retry

## Common Use Cases

#### Legal Document Processing
Extract key information from contracts, agreements, and legal documents:


#### Financial Report Analysis
Process financial documents and reports:


#### Research Paper Processing
Extract structured data from academic papers:


#### Invoice and Receipt Processing
Automate invoice and receipt data extraction:


#### Medical Record Processing
Extract patient information from medical documents:


## Workflow Components

The system is built using modular components (called "nodes") that work together:

### Input Processing
- **CSV Input**: Reads your list of document file paths
- **Chunking**: Groups documents into manageable batches

### AI Processing
- **Batch Processing**: Sends document batches to Gemini for analysis
- **Polling**: Waits for AI processing to complete

### Quality Control
- **Validation**: Ensures extracted data is properly formatted
- **Error Handling**: Manages failures and creates retry files

### Data Storage
- **Database Write**: Saves all extracted information to your database

More detail: [Workflow](./WORKFLOW.md)

## Key Features

### **Efficient Batch Processing**
- Process hundreds of documents at once
- Configurable batch sizes (default: 10 documents per batch)
- Parallel processing for maximum speed

### **AI-Powered Extraction**
- Uses Gemini's advanced language models
- Customizable prompts for different document types
- Handles various file formats (PDF, DOCX, TXT, etc.)

### **Reliable Data Storage**
- MongoDB database for secure storage
- Flexible JSON format for any data structure
- Built-in indexing for fast queries

### **Robust Error Handling**
- Automatic retry for failed documents
- Detailed logging for troubleshooting
- Failure reports for manual review

### **Easy Integration**
- Web API for external systems
- Simple CSV input format
- RESTful endpoints for automation

## Monitoring and Logging

### Log Files
- `logs/app.log`: Main application logs


### Health Checks
- Database connectivity
- Gemini API availability
- File system access

## Error Handling

### Validation Failures
- Documents with invalid data are logged
- Failure CSV is created for retry
- Detailed error messages and context

### API Failures
- Gemini API failures are handled gracefully
- Tasks are requeued with exponential backoff
- Timeout handling for long-running tasks

### Database Failures
- Connection issues are logged
- Partial writes are handled appropriately
- Transaction rollback on critical failures



## Security

### API Keys
- Stored as secrets in Exosphere
- No hardcoded credentials
- Environment variable management

### Data Privacy
- Document paths are logged (ensure no sensitive data)
- Extracted data stored securely
- Access controls on database

### Input Validation
- CSV file format validation
- File path sanitization
- Prompt injection prevention

## Troubleshooting Guide

### Common Issues and Solutions

#### **"CSV file not found" Error**
- **Problem**: The system can't find your document list file
- **Solution**: 
  - Check the file path in your CSV
  - Make sure the file exists and you have read permissions
  - Use absolute paths (e.g., `/full/path/to/file.pdf`) instead of relative paths

#### **"Gemini API Error" Messages**
- **Problem**: Gemini API calls are failing
- **Solutions**:
  - Verify your Gemini API key is correct
  - Check if you have sufficient API credits
  - Ensure you're not hitting rate limits (try smaller batch sizes)
  - Check Gemini's service status

#### **"Database Connection Failed"**
- **Problem**: Can't connect to your database
- **Solutions**:
  - Verify your DATABASE_URL is correct
  - Check if your database server is running
  - Ensure your database allows connections from your IP
  - Test connection with a database client

#### **"Validation Failed" Errors**
- **Problem**: AI extracted data doesn't match expected format
- **Solutions**:
  - Review and improve your processing prompt
  - Check if your documents are readable (not corrupted)
  - Try processing a smaller batch first
  - Look at the extracted data in logs to see what went wrong

### Debugging Steps

1. **Check the Logs**
   ```bash
   # View the main log file
   tail -f logs/app.log
   
   # Look for error messages
   grep -i error logs/app.log
   ```

2. **Verify Your Setup**
   ```bash
   # Test your environment variables
   python -c "import os; print('API keys loaded:', bool(os.getenv('GEMINI_API_KEY')))"
   


3. **Test with Sample Data**
   ```bash
   # Always test with a small batch first
   # Create a test CSV with just 2-3 documents
   # Run the processing and check results
   ```

### Getting Help

If you're still stuck:

1. **Check the Logs**: Look in `logs/app.log` for detailed error messages
2. **Review Configuration**: Double-check your `.env` file and API keys
3. **Test Components**: Try each step individually to isolate the issue
4. **Start Small**: Process just 1-2 documents first to verify everything works
5. **Check Service Status**: Verify Gemini and Exosphere services are running

### Support Resources

- **Exosphere Documentation**: Check the official Exosphere docs
- **Gemini API Docs**: Review Gemini's API documentation
- **Database Issues**: Consult your database provider's documentation
- **Log Analysis**: The log files contain detailed information about what went wrong

## Next Steps

Once you have the system working:

1. **Customize for Your Use Case**
   - Modify the processing prompt in `processing_prompt_template.txt`
   - Adjust batch sizes based on your document types
   - Set up custom validation rules

2. **Monitor and Optimize**
   - Set up monitoring for your processing jobs
   - Optimize database queries for your data patterns
   - Track processing times and success rates

3. **Production Considerations**
   - Implement proper authentication and authorization
   - Set up automated backups
   - Configure alerting for failures
   - Consider scaling options for large document volumes

4. **Testing and Quality**
   - Create comprehensive test datasets
   - Implement automated testing
   - Set up quality assurance processes
   - Document your specific workflows

## You're Ready!

You now have a complete document processing system that can handle large volumes of documents efficiently. Start with small batches, monitor the results, and gradually scale up as you become comfortable with the system.
