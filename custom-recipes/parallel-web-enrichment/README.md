# Parallel Web Enrichment Recipe

This custom recipe enriches dataset rows using the Parallel Web Systems Task API. It takes an input dataset, extracts structured information from the web for each row, and writes the enriched data to an output dataset.

## Features

- **Flexible Input**: Select any number of columns from your input dataset to use as context for web enrichment
- **Dynamic Output**: Define custom output fields with descriptions to extract specific information
- **Error Handling**: Continue processing even if individual rows fail, with optional error tracking
- **Batch Processing**: Configure batch sizes for optimal performance
- **Configurable Timeouts**: Set maximum wait times and polling intervals

## Configuration

### API Configuration

- **API Key**: Select a Parallel API key preset for authentication
- **Processor**: Choose the processing engine (core, base, or fast)

### Input Configuration

- **Input Columns**: Select one or more columns from your input dataset to use as input for the enrichment task. These columns will be provided to the API as context for extracting information.

### Output Configuration

- **Output Fields**: Define the fields you want to extract from web sources. Each field requires:
  - **Column name**: The name of the output column (e.g., "founding_year")
  - **Type**: The data type for this field (string, int, float, or bool)
  - **Description**: A description of what information to extract (e.g., "The year the company was founded")

### Advanced Settings

- **Max Wait Time**: Maximum time in seconds to wait for each task to complete (default: 300)
- **Poll Interval**: Time in seconds between status checks (default: 2)
- **Batch Size**: Number of rows to process before writing to output (0 = all at once, default: 100)
- **Continue on Error**: If enabled, processing continues even if individual rows fail. Failed rows will have empty enrichment values and an error message in the `_enrichment_error` column.

## Example Use Case

### Input Dataset

| company_name | website |
|--------------|---------|
| Stripe | stripe.com |
| OpenAI | openai.com |
| Anthropic | anthropic.com |

### Configuration

**Input Columns**: `company_name`, `website`

**Output Fields**:
- Column name: `founding_year`, Type: `string`, Description: "The year the company was founded"
- Column name: `employee_count`, Type: `string`, Description: "The approximate number of employees"
- Column name: `total_funding`, Type: `string`, Description: "The total funding raised by the company"

### Output Dataset

| company_name | website | founding_year | employee_count | total_funding |
|--------------|---------|---------------|----------------|---------------|
| Stripe | stripe.com | 2010 | 8000+ | $2.2B |
| OpenAI | openai.com | 2015 | 500+ | $11.3B |
| Anthropic | anthropic.com | 2021 | 150+ | $7.3B |

## Usage Tips

1. **Start Small**: Test with a small subset of your data first to verify the configuration
2. **Descriptive Field Definitions**: Provide clear, detailed descriptions for output fields to get better results
3. **Error Tracking**: Enable "Continue on Error" to avoid losing progress on large datasets
4. **Batch Processing**: Use smaller batch sizes for very large datasets to see progress more frequently

## Performance Considerations

- Processing time depends on the complexity of the enrichment task and the processor selected
- The "core" processor provides the most accurate results but may be slower
- The "fast" processor provides quicker results with potentially lower accuracy
- Consider the API rate limits when processing large datasets

## Troubleshooting

**Issue**: Recipe fails with "API key not configured"
- **Solution**: Ensure you have created a Parallel API key preset in the plugin settings

**Issue**: Tasks are timing out
- **Solution**: Increase the "Max Wait Time" setting or simplify your output field definitions

**Issue**: Empty output values
- **Solution**: Check the `_enrichment_error` column (if "Continue on Error" is enabled) for error messages. Verify that your input columns contain valid data.
