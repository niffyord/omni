import asyncio
import os
from dotenv import load_dotenv

from agents import Agent, CodeInterpreterTool, Runner, trace

# Load environment variables from .env file
load_dotenv()


async def main():
    agent = Agent(
        name="Code interpreter",
        model="gpt-4.1",
        instructions="You love doing math.",
        tools=[
            CodeInterpreterTool(
                tool_config={"type": "code_interpreter", "container": {"type": "auto"}},
            )
        ],
    )

    with trace("Code interpreter example"):
        print("Solving math problem...")
        result = Runner.run_streamed(agent, "Wuse the python tool to calculate what is 4 * 3.82. and then find its square root and then find the square root of that result")
        async for event in result.stream_events():
            if (
                event.type == "run_item_stream_event"
                and event.item.type == "tool_call_item"
                and event.item.raw_item.type == "code_interpreter_call"
            ):
                print(f"Code interpreter code:\n```\n{event.item.raw_item.code}\n```\n")
            elif event.type == "run_item_stream_event":
                print(f"Other event: {event.item.type}")

        print(f"Final output: {result.final_output}")


if __name__ == "__main__":
    asyncio.run(main())