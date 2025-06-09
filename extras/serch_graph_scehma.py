"""
Example of Search Graph
"""

import os
from typing import List

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from scrapegraphai.graphs import SearchGraph

load_dotenv()


# ************************************************
# Define the configuration for the graph
# ************************************************
class CeoName(BaseModel):
    ceo_name: str = Field(description="The name and surname of the ceo")


class Ceos(BaseModel):
    names: List[CeoName]


openai_key = os.getenv("OPENAI_APIKEY")

graph_config = {
    "llm": {
        "api_key": openai_key,
        "model": "openai/gpt-4o",
    },
    "max_results": 2,
    "verbose": True,
}

# ************************************************
# Create the SearchGraph instance and run it
# ************************************************

search_graph = SearchGraph(
    prompt="Who is the ceo of Appke?",
    schema=Ceos,
    config=graph_config,
)

result = search_graph.run()
print(result)
