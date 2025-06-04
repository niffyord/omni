from agents import Agent, WebSearchTool
from agents.model_settings import ModelSettings

INSTRUCTIONS = (
    "You are a research assistant specialized in cryptocurrency markets. Given a search term, use web search to gather the most recent market news."
    " Summarize the key points in two to three paragraphs, under 300 words, focusing on major price movements, regulatory updates, and notable market sentiment."
)

search_agent = Agent(
    name="CryptoSearchAgent",
    instructions=INSTRUCTIONS,
    tools=[WebSearchTool()],
    model="gpt-4.1",
    model_settings=ModelSettings(tool_choice="required"),
)