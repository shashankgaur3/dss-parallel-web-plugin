# This file is the implementation of custom agent tool parallel-web-system-search
from dataiku.llm.agent_tools import BaseAgentTool
from parallel import Parallel
import logging

logger = logging.getLogger(__name__)


class CustomAgentTool(BaseAgentTool):
    """
    Parallel Web Search Agent Tool

    This tool enables AI agents to search the web using Parallel's Search API,
    which provides intelligent search results with excerpts from relevant web pages.
    """

    def set_config(self, config, plugin_config):
        """
        Initialize the tool with configuration.

        Args:
            config: Tool configuration from Dataiku
            plugin_config: Plugin-level configuration
        """
        self.config = config
        self.plugin_config = plugin_config

        # Get API key from config
        self.api_key = self.config.get("api_auth", {}).get("parallel-api-key", "")
        if not self.api_key:
            raise ValueError("Parallel API key not configured. Please set up authentication in plugin settings.")

        # Get search configuration parameters
        self.mode = self.config.get("mode", "agentic")
        self.max_results = int(self.config.get("max_results", 5))
        self.max_chars_per_excerpt = int(self.config.get("max_chars_per_excerpt", 1000))

        # Validate parameters
        if self.max_results < 1 or self.max_results > 20:
            raise ValueError("max_results must be between 1 and 20")
        if self.max_chars_per_excerpt < 100 or self.max_chars_per_excerpt > 10000:
            raise ValueError("max_chars_per_excerpt must be between 100 and 10000")

        # Initialize Parallel client
        self.client = Parallel(api_key=self.api_key)

    def get_descriptor(self, tool):
        """
        Return the tool descriptor for the AI agent.

        Returns:
            dict: Tool descriptor with description and input schema
        """
        return {
            "description": "Search the web using Parallel's intelligent search API. Provide an objective describing what information you're looking for. Returns relevant web pages with titles, URLs, and text excerpts.",
            "inputSchema": {
                "$id": "https://parallel.ai/agents/tools/search/input",
                "title": "Input for Parallel Web Search",
                "type": "object",
                "properties": {
                    "objective": {
                        "type": "string",
                        "description": "A clear description of what information you're looking for (e.g., 'When was the United Nations established? Prefer UN's websites.')"
                    }
                },
                "required": ["objective"]
            }
        }

    def invoke(self, input, trace):
        """
        Execute the search using Parallel's Search API.

        Args:
            input: Input parameters from the AI agent
            trace: Trace object for logging execution details

        Returns:
            dict: Search results with output and sources
        """
        args = input["input"]

        # Extract objective from agent input
        objective = args.get("objective", "")

        if not objective:
            return {
                "output": "Error: 'objective' parameter is required.",
                "sources": [{
                    "toolCallDescription": "Search failed: missing objective"
                }]
            }

        try:
            logger.info(f"Executing Parallel search with objective: {objective}")
            logger.info(f"Search configuration - mode: {self.mode}, max_results: {self.max_results}, max_chars_per_excerpt: {self.max_chars_per_excerpt}")

            # Call Parallel Search API with configured parameters
            search = self.client.beta.search(
                objective=objective,
                mode=self.mode,
                max_results=self.max_results,
                excerpts={"max_chars_per_result": self.max_chars_per_excerpt}
            )

            # Format results
            results_list = []
            sources_list = []

            result_count = 0
            for result in search.results:
                if result_count >= self.max_results:
                    break

                result_text = f"**{result.title}**\nURL: {result.url}\n\n"

                # Add excerpts
                if hasattr(result, 'excerpts') and result.excerpts:
                    result_text += "Excerpts:\n"
                    for i, excerpt in enumerate(result.excerpts, 1):
                        # Truncate excerpt if needed
                        excerpt_text = excerpt[:self.max_chars_per_excerpt] if len(excerpt) > self.max_chars_per_excerpt else excerpt
                        result_text += f"{i}. {excerpt_text}\n\n"

                results_list.append(result_text)

                # Add to sources for traceability
                sources_list.append({
                    "title": result.title,
                    "url": result.url,
                    "toolCallDescription": f"Search result: {result.title}"
                })

                result_count += 1

            # Combine all results
            if results_list:
                output = f"Found {result_count} result(s) for objective: '{objective}'\n\n" + "\n---\n\n".join(results_list)
            else:
                output = f"No results found for objective: '{objective}'"

            logger.info(f"Search completed successfully with {result_count} results")

            return {
                "output": output,
                "sources": sources_list if sources_list else [{
                    "toolCallDescription": f"Searched web for: {objective}"
                }]
            }

        except Exception as e:
            logger.error(f"Error during Parallel search: {str(e)}", exc_info=True)
            return {
                "output": f"Error during search: {str(e)}",
                "sources": [{
                    "toolCallDescription": f"Search failed: {str(e)}"
                }]
            }

    def load_sample_query(self, tool):
        """
        Provide a sample query for testing the tool.

        Returns:
            dict: Sample query parameters
        """
        return {
            "objective": "When was the United Nations established? Prefer UN's websites."
        }
