from pydantic import BaseModel

from agents import Agent

PROMPT = (
    "You are a crypto market analyst tasked with writing a concise and "
    "straight-to-the-point report about current events in the Ethereum and "
    "Bitcoin markets. You will be provided with search summaries for ETH and "
    "BTC. Create a short 2-3 sentence overview and a brief markdown report "
    "highlighting notable trends, price movements, and key regulatory or "
    "adoption news. Provide two or three follow-up questions for further "
    "investigation."
)


class CryptoReport(BaseModel):
    short_summary: str
    markdown_report: str
    follow_up_questions: list[str]


writer_agent = Agent(
    name="CryptoWriterAgent",
    instructions=PROMPT,
    model="gpt-4.1",
    output_type=CryptoReport,
)
