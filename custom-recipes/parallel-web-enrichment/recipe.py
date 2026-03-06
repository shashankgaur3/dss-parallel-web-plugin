"""
Parallel Web Enrichment Recipe

This recipe enriches dataset rows using the Parallel Web Systems Task API.
It takes an input dataset, processes each row through the Task API to extract
structured information from the web, and writes the enriched data to an output dataset.
"""

import dataiku
import logging
import time
import pandas as pd
from parallel import Parallel
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
from parallelwebsystems.utils import (
    map_type_to_json_schema,
    map_type_to_dataiku,
    normalize_column_name
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def row_to_dict_with_nan_handling(row):
    """
    Convert pandas Series to dict, replacing NaN/None with empty string.

    Args:
        row: pandas Series

    Returns:
        dict: Dictionary with NaN values replaced by empty strings
    """
    row_dict = row.to_dict()
    # Replace NaN and None values with empty string
    for key, value in row_dict.items():
        if pd.isna(value) or value is None:
            row_dict[key] = ''
    return row_dict


def wait_for_task_completion(client, task_run_id, max_wait_seconds=300, poll_interval=2):
    """
    Wait for a task run to complete by polling its status.

    Args:
        client: Parallel API client instance
        task_run_id: Task run ID to monitor
        max_wait_seconds: Maximum time to wait in seconds
        poll_interval: Time between status checks in seconds

    Returns:
        Task result object

    Raises:
        Exception: If task fails or times out
    """
    terminal_success = {"completed"}
    terminal_failure = {"failed", "cancelled"}

    start_time = time.time()

    while True:
        elapsed = time.time() - start_time

        # Check for timeout
        if elapsed > max_wait_seconds:
            raise Exception(f"Task timed out after {max_wait_seconds} seconds")

        # Retrieve task status (response can be object or dict)
        task = client.task_run.retrieve(task_run_id)

        # Extract status and is_active
        if hasattr(task, 'status'):
            status = str(task.status).lower().strip() if task.status else ''
            is_active = bool(task.is_active) if hasattr(task, 'is_active') else True
        elif hasattr(task, 'get'):
            status = str(task.get('status', '')).lower().strip()
            is_active = bool(task.get('is_active', True))
        else:
            status = ''
            is_active = True

        # Check for completion
        if status in terminal_success or (not is_active and status not in terminal_failure):
            return task

        # Check for failure
        if status in terminal_failure:
            # Extract error message
            if hasattr(task, 'error'):
                error_msg = task.error or "Unknown error"
            elif hasattr(task, 'get'):
                error_msg = task.get('error') or task.get('message') or task.get('reason') or "Unknown error"
            else:
                error_msg = "Unknown error"
            raise Exception(f"Task failed with status '{status}': {error_msg}")

        # Wait before next poll
        time.sleep(poll_interval)


def extract_task_output(task_result):
    """
    Extract output data from task result.

    Args:
        task_result: Task result object or dict from result API

    Returns:
        dict: Output data content
    """
    # Extract output field (can be object or dict)
    if hasattr(task_result, 'output'):
        output = task_result.output
    elif hasattr(task_result, 'get'):
        output = task_result.get('output', {})
    else:
        output = {}

    # Extract content from output (can be object or dict)
    if hasattr(output, 'content'):
        content = output.content
    elif isinstance(output, dict):
        content = output.get('content', {})
    else:
        content = {}

    # Convert content to dict if it's an object
    if hasattr(content, 'model_dump'):
        content = content.model_dump()
    elif hasattr(content, 'dict'):
        content = content.dict()
    elif hasattr(content, '__dict__'):
        content = content.__dict__

    # Return empty dict if content is not a dict
    if not isinstance(content, dict):
        return {}

    return content


def build_task_spec(input_columns, output_fields):
    """
    Build the task specification for the Parallel Task API.

    Args:
        input_columns: List of input column names
        output_fields: List of dicts with 'name', 'type', and 'description'

    Returns:
        dict: Task specification
    """
    # Build input schema
    input_properties = {}
    for col in input_columns:
        input_properties[col] = {"type": "string"}

    # Build output schema
    output_properties = {}
    for field in output_fields:
        # Field names are already normalized at this point
        field_name = field.get('name', '').strip()
        field_type = field.get('type', 'string').strip()
        field_desc = field.get('description', '').strip()

        if not field_name:
            continue

        # Map output field type to JSON schema type
        json_type = map_type_to_json_schema(field_type)

        output_properties[field_name] = {
            "type": json_type,
            "description": field_desc
        }

    task_spec = {
        "input_schema": {
            "type": "json",
            "json_schema": {
                "type": "object",
                "properties": input_properties
            }
        },
        "output_schema": {
            "type": "json",
            "json_schema": {
                "type": "object",
                "properties": output_properties
            }
        }
    }

    return task_spec


def enrich_row(client, row, input_columns, task_spec, processor, max_wait_seconds, poll_interval):
    """
    Enrich a single row using the Parallel Task API.

    Args:
        client: Parallel API client instance
        row: Input row as dict
        input_columns: List of input column names
        task_spec: Task specification
        processor: Processor to use
        max_wait_seconds: Maximum wait time
        poll_interval: Poll interval

    Returns:
        dict: Enriched output data
    """
    # Build input data from row
    input_data = {}
    for col in input_columns:
        value = row.get(col, '')
        # Convert to string if not already
        input_data[col] = str(value) if value is not None else ''

    # Create task run
    task_run = client.beta.task_run.create(
        input=input_data,
        task_spec=task_spec,
        processor=processor
    )

    # Extract task run ID from response (response can be object or dict)
    if hasattr(task_run, 'run_id'):
        task_run_id = task_run.run_id
    elif hasattr(task_run, 'get'):
        task_run_id = task_run.get('run_id')
    else:
        task_run_id = None

    if not task_run_id:
        raise Exception("Could not extract run_id from task create response")

    # Wait for completion
    wait_for_task_completion(client, task_run_id, max_wait_seconds, poll_interval)

    # Get the result with output data
    task_result = client.beta.task_run.result(run_id=task_run_id)

    # Extract output
    output_data = extract_task_output(task_result)

    return output_data


def main():
    """
    Main recipe execution logic.
    """
    logger.info("Starting Parallel Web Enrichment Recipe")

    # Get recipe configuration
    recipe_config = get_recipe_config()

    # Extract parameters
    api_key = recipe_config.get('api_key_preset', {}).get('parallel-api-key', '')

    if not api_key:
        raise Exception("API key not configured. Please set the API key in the recipe settings.")

    processor = recipe_config.get('processor', 'base')
    input_columns = recipe_config.get('input_columns', [])
    output_fields = recipe_config.get('output_fields', [])
    max_wait_seconds = int(recipe_config.get('max_wait_seconds', 300))
    poll_interval_seconds = int(recipe_config.get('poll_interval_seconds', 2))
    batch_size = int(recipe_config.get('batch_size', 100))
    continue_on_error = bool(recipe_config.get('continue_on_error', True))

    logger.info(f"Configuration: processor={processor}, input_columns={input_columns}, "
                f"output_fields={len(output_fields)}, batch_size={batch_size}")

    # Validate configuration
    if not input_columns:
        raise Exception("No input columns selected. Please select at least one input column.")

    if not output_fields:
        raise Exception("No output fields defined. Please define at least one output field.")

    # Normalize all field names once (overwrite in place to avoid multiple normalization calls)
    for field in output_fields:
        if 'name' in field and field['name']:
            field['name'] = normalize_column_name(field['name'])

    # Initialize Parallel client
    client = Parallel(api_key=api_key)
    logger.info("Parallel client initialized successfully")

    # Build task specification
    task_spec = build_task_spec(input_columns, output_fields)
    logger.info(f"Task specification built with {len(input_columns)} input columns and "
                f"{len(task_spec['output_schema']['json_schema']['properties'])} output fields")

    # Get input and output datasets
    input_dataset = dataiku.Dataset(get_input_names_for_role('input_dataset')[0])
    output_dataset = dataiku.Dataset(get_output_names_for_role('output_dataset')[0])

    logger.info(f"Reading from dataset: {input_dataset.name}")
    logger.info(f"Writing to dataset: {output_dataset.name}")

    # Get output field names (already normalized)
    output_field_names = [field.get('name', '').strip() for field in output_fields if field.get('name', '').strip()]

    # Read input dataset
    input_df = input_dataset.get_dataframe()
    total_rows = len(input_df)
    logger.info(f"Processing {total_rows} rows")

    # Build and set output schema
    # Start with input dataset schema
    input_schema = input_dataset.read_schema()
    output_schema_columns = []

    # Add all input columns
    for col in input_schema:
        output_schema_columns.append(col)

    # Add output enrichment fields (names are already normalized)
    for field in output_fields:
        field_name = field.get('name', '').strip()
        field_type = field.get('type', 'string').strip()

        if not field_name:
            continue

        # Map to Dataiku column types
        dataiku_type = map_type_to_dataiku(field_type)

        output_schema_columns.append({
            'name': field_name,
            'type': dataiku_type
        })

    # Add error column if continue_on_error is enabled
    if continue_on_error:
        output_schema_columns.append({
            'name': 'error_message',
            'type': 'string'
        })

    # Set the schema on output dataset
    output_dataset.write_schema(output_schema_columns)
    logger.info(f"Output schema set with {len(output_schema_columns)} columns")

    # Process rows and enrich
    processed_count = 0
    error_count = 0

    # Determine actual batch size (0 means process all at once)
    effective_batch_size = batch_size if batch_size > 0 else total_rows

    with output_dataset.get_writer() as writer:
        batch = []

        for idx, row in input_df.iterrows():
            try:
                logger.info(f"Processing row {idx + 1}/{total_rows}")

                # Convert row to dict with NaN handling
                row_dict = row_to_dict_with_nan_handling(row)

                # Enrich the row
                output_data = enrich_row(
                    client=client,
                    row=row_dict,
                    input_columns=input_columns,
                    task_spec=task_spec,
                    processor=processor,
                    max_wait_seconds=max_wait_seconds,
                    poll_interval=poll_interval_seconds
                )

                # Combine input row with enriched data
                enriched_row = row_dict.copy()
                for field_name in output_field_names:
                    enriched_row[field_name] = output_data.get(field_name, '')

                batch.append(enriched_row)
                processed_count += 1

                logger.info(f"Row {idx + 1} enriched successfully")

            except Exception as e:
                error_count += 1
                error_msg = str(e)
                logger.error(f"Error processing row {idx + 1}: {error_msg}")

                if continue_on_error:
                    # Add row with empty enrichment values to batch
                    enriched_row = row_to_dict_with_nan_handling(row)
                    for field_name in output_field_names:
                        enriched_row[field_name] = ''
                    enriched_row['error_message'] = error_msg
                    batch.append(enriched_row)
                else:
                    raise

            # Write batch when it reaches the batch size
            if len(batch) >= effective_batch_size:
                logger.info(f"Writing batch of {len(batch)} rows to output dataset")
                for enriched_row in batch:
                    writer.write_row_dict(enriched_row)
                batch = []

        # Write any remaining rows in the final batch
        if batch:
            logger.info(f"Writing final batch of {len(batch)} rows to output dataset")
            for enriched_row in batch:
                writer.write_row_dict(enriched_row)

    logger.info(f"Recipe completed. Processed: {processed_count}, Errors: {error_count}, Total: {total_rows}")


if __name__ == "__main__":
    main()
