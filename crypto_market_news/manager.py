from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from rich.console import Console

from agents import Runner, custom_span, gen_trace_id, trace

from .agents.search_agent import search_agent
from .agents.writer_agent import CryptoReport, writer_agent
from .printer import Printer


class CryptoNewsManager:
    def __init__(self) -> None:
        self.console = Console()
        self.printer = Printer(self.console)

    async def run(self) -> None:
        trace_id = gen_trace_id()
        with trace("Crypto news trace", trace_id=trace_id):
            self.printer.update_item(
                "trace_id",
                f"View trace: https://platform.openai.com/traces/trace?trace_id={trace_id}",
                is_done=True,
                hide_checkmark=True,
            )

            self.printer.update_item(
                "starting",
                "Gathering crypto market news...",
                is_done=True,
                hide_checkmark=True,
            )
            search_terms = [
                "latest Ethereum market news",
                "latest Bitcoin market news",
            ]
            search_results = await self._perform_searches(search_terms)
            report = await self._write_report(search_results)

            final_report = f"Report summary\n\n{report.short_summary}"
            self.printer.update_item("final_report", final_report, is_done=True)

            self.printer.end()

        print("\n\n=====REPORT=====\n\n")
        print(f"Report: {report.markdown_report}")

        output_dir = Path(__file__).parent / "outputs"
        output_dir.mkdir(exist_ok=True)
        filename = output_dir / "crypto_news.json"
        data = {
            "short_summary": report.short_summary,
            "markdown_report": report.markdown_report,
        }
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        print(f"Saved report to {filename}")

    async def _perform_searches(self, search_terms: list[str]) -> list[str]:
        with custom_span("Search the web"):
            self.printer.update_item("searching", "Searching...")
            num_completed = 0
            tasks = [asyncio.create_task(self._search(term)) for term in search_terms]
            results = []
            for task in asyncio.as_completed(tasks):
                result = await task
                if result is not None:
                    results.append(result)
                num_completed += 1
                self.printer.update_item(
                    "searching", f"Searching... {num_completed}/{len(tasks)} completed"
                )
            self.printer.mark_item_done("searching")
            return results

    async def _search(self, term: str) -> str | None:
        input = f"Search term: {term}"
        try:
            result = await Runner.run(search_agent, input)
            return str(result.final_output)
        except Exception:
            return None

    async def _write_report(self, search_results: list[str]) -> CryptoReport:
        self.printer.update_item("writing", "Compiling report...")
        input = f"Search summaries: {search_results}"
        result = Runner.run_streamed(writer_agent, input)
        update_messages = [
            "Compiling report...",
            "Writing analysis...",
            "Finalizing...",
        ]

        last_update = time.time()
        next_message = 0
        async for _ in result.stream_events():
            if time.time() - last_update > 5 and next_message < len(update_messages):
                self.printer.update_item("writing", update_messages[next_message])
                next_message += 1
                last_update = time.time()

        self.printer.mark_item_done("writing")
        return result.final_output_as(CryptoReport)