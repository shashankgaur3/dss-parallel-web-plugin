import json
import logging
import time

from dataiku.connector import Connector
from parallel import Parallel
from pydantic import BaseModel, Field, create_model
from parallelwebsystems.utils import (
    map_type_to_python,
    normalize_column_name,
    parse_and_validate_fields,
    convert_to_dict
)

# Set up logging
logger = logging.getLogger(__name__)


class MyConnector(Connector):
    """
    Custom Dataiku connector for Parallel FindAll API.

    This connector fetches structured web data using Parallel's FindAll API,
    with optional enrichment capabilities.
    """

    def __init__(self, config, plugin_config):
        """
        Initialize the connector with configuration parameters.

        Args:
            config: Connector configuration from Dataiku
            plugin_config: Plugin-level configuration

        Raises:
            Exception: If required parameters are missing or invalid
        """
        Connector.__init__(self, config, plugin_config)
        logger.info("Initializing Parallel FindAll connector")

        self.api_key = self.config.get("api_auth", {}).get("parallel-api-key", "")
        self.objective = (self.config.get("objective") or "").strip()
        self.generator = (self.config.get("generator") or "base").strip()
        self.match_limit = int(self.config.get("match_limit", 10))
        self.enable_enrichments = bool(self.config.get("enable_enrichments", False))
        self.include_citations = bool(self.config.get("include_citations", False))
        self.enrichment_fields = self._parse_enrichment_fields() if self.enable_enrichments else []
        self.poll_interval_seconds = max(1, int(self.config.get("poll_interval_seconds", 5)))
        self.max_wait_seconds = max(5, int(self.config.get("max_wait_seconds", 300)))

        logger.debug(f"Configuration: generator={self.generator}, match_limit={self.match_limit}, "
                    f"enable_enrichments={self.enable_enrichments}, include_citations={self.include_citations}")

        if not self.api_key:
            logger.error("Missing API key")
            raise Exception("Missing required parameter: api_auth preset / parallel-api-key")
        if not self.objective:
            logger.error("Missing objective")
            raise Exception("Missing required parameter: objective")
        if self.match_limit < 1:
            logger.error(f"Invalid match_limit: {self.match_limit}")
            raise Exception("Invalid match_limit: must be a positive integer")

        logger.info(f"Connector initialized successfully with objective: {self.objective[:50]}...")

    def get_read_schema(self):
        """Schema is dynamically determined from API response."""
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        """
        Generate rows by calling the Parallel FindAll API.

        This method orchestrates the entire FindAll workflow:
        1. Ingest objective and get schema
        2. Create and start FindAll run
        3. Wait for completion
        4. Optionally run enrichment
        5. Extract and yield results

        Args:
            dataset_schema: Dataiku dataset schema (optional)
            dataset_partitioning: Partitioning configuration (optional)
            partition_id: Partition identifier (optional)
            records_limit: Maximum number of records to return (-1 for unlimited)

        Yields:
            dict: Row data as dictionary
        """
        logger.info("Starting generate_rows")
        client = Parallel(api_key=self.api_key)

        logger.info("Step 1: Getting FindAll schema")
        findall_schema = self._get_findall_schema(client)

        logger.info("Step 2: Starting FindAll run")
        findall_id = self._start_run(client, findall_schema)
        logger.info(f"FindAll run created with ID: {findall_id}")

        logger.info("Step 3: Waiting for FindAll run to complete")
        self._wait_for_run(client, findall_id)

        if self.enrichment_fields:
            logger.info(f"Step 4: Running enrichment with {len(self.enrichment_fields)} fields")
            self._run_enrichment(client, findall_id)
            logger.info("Step 5: Waiting for enrichment to complete")
            self._wait_for_run(client, findall_id)

        logger.info("Step 6: Fetching results")
        run_result = client.beta.findall.result(findall_id=findall_id)

        count = 0
        logger.info("Step 7: Extracting and yielding rows")
        for row in self._extract_rows(run_result):
            yield row
            count += 1
            if records_limit >= 0 and count >= records_limit:
                logger.info(f"Reached records limit: {records_limit}")
                break

        logger.info(f"Completed. Total rows yielded: {count}")

    def get_writer(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, write_mode="OVERWRITE"):
        """Writer not supported for this connector."""
        raise NotImplementedError

    def get_partitioning(self):
        """Partitioning not supported for this connector."""
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """Empty list as partitioning is not supported.""" 
        return []

    def partition_exists(self, partitioning, partition_id):
        """Partition existence check not supported."""
        raise NotImplementedError

    def get_records_count(self, partitioning=None, partition_id=None):
        """Record counting not supported."""
        raise NotImplementedError

    def _get_findall_schema(self, client):
        """
        Call Parallel FindAll ingest API to get the schema.

        Args:
            client: Parallel API client instance

        Returns:
            dict: FindAll schema containing objective, entity_type, and match_conditions

        Raises:
            Exception: If ingest response format is unexpected
        """
        logger.debug(f"Calling ingest API with objective: {self.objective[:100]}...")
        ingest = client.beta.findall.ingest(objective=self.objective)
        schema = self._to_plain(ingest)

        if not isinstance(schema, dict):
            logger.error(f"Unexpected ingest response type: {type(schema)}")
            raise Exception("Unexpected ingest response format")

        logger.debug(f"Received schema with entity_type: {schema.get('entity_type')}")
        return schema

    def _start_run(self, client, findall_schema):
        """
        Create and start a FindAll run.

        Args:
            client: Parallel API client instance
            findall_schema: Schema from ingest API containing required fields

        Returns:
            str: FindAll run ID

        Raises:
            Exception: If schema is missing required fields or run creation fails
        """
        objective = findall_schema.get("objective")
        entity_type = findall_schema.get("entity_type")
        match_conditions = findall_schema.get("match_conditions")

        missing = []
        if not objective:
            missing.append("objective")
        if not entity_type:
            missing.append("entity_type")
        if not match_conditions:
            missing.append("match_conditions")
        if missing:
            logger.error(f"Schema missing required fields: {missing}")
            raise Exception("FindAll ingest/schema is missing required fields: %s" % ", ".join(missing))

        logger.debug(f"Creating FindAll run: generator={self.generator}, match_limit={self.match_limit}")
        run = client.beta.findall.create(
            objective=objective,
            entity_type=entity_type,
            match_conditions=match_conditions,
            generator=self.generator,
            match_limit=self.match_limit,
        )

        # Extract findall_id from response
        run_data = self._to_plain(run)
        findall_id = run_data.get("findall_id") if isinstance(run_data, dict) else None

        if not findall_id:
            logger.error("Could not extract findall_id from create response")
            raise Exception("Could not find findall_id in create response")

        logger.debug(f"FindAll run started successfully: {findall_id}")
        return str(findall_id)

    def _wait_for_run(self, client, findall_id):
        """
        Poll FindAll run status until completion or failure.

        Args:
            client: Parallel API client instance
            findall_id: FindAll run ID to monitor

        Raises:
            Exception: If run fails or times out
        """
        terminal_success = set(["completed", "succeeded", "success", "done", "finished"])
        terminal_failure = set(["failed", "error", "cancelled", "canceled", "timed_out", "timeout"])
        started = time.time()
        last_logged_status = None

        logger.debug(f"Waiting for run {findall_id} to complete (max wait: {self.max_wait_seconds}s)")

        while True:
            run = client.beta.findall.retrieve(findall_id=findall_id)
            run_data = self._to_plain(run)

            # Extract status from run_data (already validated as dict or converted to dict)
            status_obj = run_data.get("status", {}) if isinstance(run_data, dict) else {}
            if isinstance(status_obj, dict):
                status = str(status_obj.get("status", "")).lower().strip()
                is_active = bool(status_obj.get("is_active", True))
            else:
                status = str(status_obj).lower().strip()
                is_active = True

            # Log status changes
            if status != last_logged_status:
                elapsed = time.time() - started
                logger.info(f"Run {findall_id} status: {status} (elapsed: {elapsed:.1f}s)")
                last_logged_status = status

            if status in terminal_success or (not is_active and status not in terminal_failure):
                logger.info(f"Run {findall_id} completed successfully with status: {status}")
                return

            if status in terminal_failure:
                # Extract error message (run_data type already checked above)
                error_msg = (run_data.get("error") or run_data.get("message") or
                           run_data.get("reason")) if isinstance(run_data, dict) else None
                logger.error(f"Run {findall_id} failed with status '{status}': {error_msg}")
                raise Exception("FindAll run %s ended with status '%s': %s" % (findall_id, status, error_msg))

            elapsed = time.time() - started
            if elapsed > self.max_wait_seconds:
                logger.error(f"Run {findall_id} timed out after {self.max_wait_seconds}s (status: {status})")
                raise Exception(
                    "Timed out while waiting for run %s after %ss (last status: %s)"
                    % (findall_id, self.max_wait_seconds, status or "unknown")
                )

            time.sleep(self.poll_interval_seconds)

    def _run_enrichment(self, client, findall_id):
        """
        Run enrichment on FindAll results.

        Args:
            client: Parallel API client instance
            findall_id: FindAll run ID to enrich
        """
        logger.debug(f"Building enrichment schema with {len(self.enrichment_fields)} fields")
        output_schema = self._build_enrichment_output_schema()

        logger.info(f"Starting enrichment for run {findall_id}")
        client.beta.findall.enrich(
            findall_id=findall_id,
            processor=self.generator,
            output_schema=output_schema,
        )
        logger.debug("Enrichment request submitted successfully")

    def _build_enrichment_output_schema(self):
        """
        Build the output schema for enrichment from enrichment_fields configuration.

        Returns:
            dict: Output schema in the format expected by Parallel API

        Raises:
            Exception: If enrichment_fields is empty or invalid
        """
        if not self.enrichment_fields:
            logger.error("Attempted to build enrichment schema with empty fields")
            raise Exception("enrichment_fields must be a non-empty list")

        field_definitions = {}
        for field in self.enrichment_fields:
            field_name = field["name"]
            description = field["description"]
            type_name = field["type"]
            field_type = self._map_enrichment_type(type_name)

            field_definitions[field_name] = (field_type, Field(description=description))
            logger.debug(f"Added enrichment field: {field_name} ({type_name})")

        # Create the Pydantic model
        model = create_model("FindallEnrichmentOutput", __base__=BaseModel, **field_definitions)

        # Convert to JSON schema and wrap in the expected format
        json_schema = model.model_json_schema()

        logger.debug(f"Generated enrichment schema with {len(field_definitions)} fields")
        return {
            "json_schema": json_schema,
            "type": "json"
        }

    def _map_enrichment_type(self, type_name):
        """
        Map enrichment type string to Python type.

        Args:
            type_name: Type name string (string, int, float, bool)

        Returns:
            type: Corresponding Python type

        Raises:
            ValueError: If type_name is not supported
        """
        try:
            return map_type_to_python(type_name)
        except ValueError as e:
            logger.error(f"Unsupported enrichment field type: {type_name}")
            raise Exception(str(e))

    def _extract_rows(self, payload):
        """
        Extract and normalize rows from FindAll API result payload.

        Only includes candidates with match_status = "matched".

        Args:
            payload: API response payload

        Yields:
            dict: Normalized row data
        """
        payload = self._to_plain(payload)

        candidates = payload.get("candidates", []) if isinstance(payload, dict) else []
        if not isinstance(candidates, list):
            candidates = [candidates]

        logger.debug(f"Extracting rows from {len(candidates)} total candidates")

        matched_count = 0
        for candidate in candidates:
            # Filter: only include matched candidates
            if isinstance(candidate, dict):
                match_status = str(candidate.get("match_status", "")).lower()
                if match_status != "matched":
                    logger.debug(f"Skipping candidate with match_status: {match_status}")
                    continue

                # Remove basis/citation if not requested
                if not self.include_citations and "basis" in candidate:
                    candidate = dict(candidate)
                    candidate.pop("basis", None)
                    logger.debug("Removed basis/citation from candidate")

            candidate_row = self._normalize_row(candidate)
            matched_count += 1
            yield candidate_row

        logger.info(f"Extracted {matched_count} matched candidates from {len(candidates)} total candidates")

    def _normalize_row(self, value):
        """
        Normalize a candidate value into a flat dictionary suitable for Dataiku dataset.

        Complex objects are JSON-serialized. The 'basis' field is moved to the end
        of the row if present. Enrichment fields from 'output' are extracted as separate columns.

        Args:
            value: Candidate value (dict, list, or scalar)

        Returns:
            dict: Normalized row data
        """
        if isinstance(value, dict):
            row = {}
            basis_value = None
            output_value = None

            # Process all keys, but save basis and output for special handling
            for key, item in value.items():
                if key == "basis":
                    basis_value = item
                    continue
                elif key == "output":
                    output_value = item
                    continue

                # Serialize complex objects
                row[str(key)] = item if (item is None or isinstance(item, (str, int, float, bool))) else json.dumps(item)

            # Extract enrichment fields from output
            if output_value is not None and self.enrichment_fields:
                enrichment_columns = self._extract_enrichment_columns(output_value)
                row.update(enrichment_columns)

            # Add basis at the end if it exists
            if basis_value is not None:
                row["basis"] = basis_value if isinstance(basis_value, (str, int, float, bool)) else json.dumps(basis_value)

            return row

        # Handle non-dict values
        if isinstance(value, list):
            return {"value": json.dumps(value)}
        return {"value": value}

    def _extract_enrichment_columns(self, output_value):
        """
        Extract enrichment fields from the output object as separate columns.

        The output object contains fields with structure:
        {"Field Name": {"type": "enrichment", "value": <actual_value>}}

        Args:
            output_value: The output object from the API response

        Returns:
            dict: Extracted enrichment fields with normalized column names
        """
        enrichment_columns = {}

        # Convert output_value to dict if it's a string
        if isinstance(output_value, str):
            try:
                output_value = json.loads(output_value)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Could not parse output value as JSON: {output_value}")
                return enrichment_columns

        if not isinstance(output_value, dict):
            logger.debug(f"Output value is not a dict: {type(output_value)}")
            return enrichment_columns

        # Extract enrichment fields
        for field_name, field_data in output_value.items():
            if isinstance(field_data, dict) and field_data.get("type") == "enrichment":
                # Normalize column name
                normalized_name = normalize_column_name(field_name)
                value = field_data.get("value")

                # Store the value (already in correct type)
                enrichment_columns[normalized_name] = value
                logger.debug(f"Extracted enrichment field: {field_name} -> {normalized_name} = {value}")

        return enrichment_columns

    def _parse_enrichment_fields(self):
        """
        Parse and validate enrichment fields configuration.

        Returns:
            list: List of validated enrichment field definitions

        Raises:
            Exception: If configuration is invalid
        """
        raw_fields = self.config.get("enrichment_fields")
        try:
            return parse_and_validate_fields(raw_fields)
        except ValueError as e:
            logger.error(f"Invalid enrichment fields configuration: {e}")
            raise Exception(str(e))

    def _to_plain(self, value):
        """
        Convert API response objects to plain Python types.

        Args:
            value: API response value

        Returns:
            Plain Python type (dict, list, str, int, float, bool, or original value)
        """
        return convert_to_dict(value)
