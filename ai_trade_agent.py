# ──────────────────────────────────────────────────────────────────────────────
# OmniTrader – single-symbol loop (ETH/USDT:USDT)  •  v0.6  •  2025-04-24
#  Implements all prompt, coordination, and runtime tweaks discussed.
# ──────────────────────────────────────────────────────────────────────────────
# Requires colorama for colored terminal logs
import os, time, json, asyncio, logging, re
from dotenv import load_dotenv
from typing import Optional, Literal, Any # Added Optional, Literal, Any
from pydantic import BaseModel, ConfigDict # Added BaseModel and ConfigDict

load_dotenv()

# Colorama for colored logs
try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    COLORAMA = True
except ImportError:
    COLORAMA = False
    class Dummy:
        RESET = CYAN = MAGENTA = GREEN = RED = YELLOW = BLUE = WHITE = BRIGHT = DIM = NORMAL = ''
    Fore = Style = Dummy()

import ccxt
# Updated agents import for AgentHooks, RunContextWrapper, trace, Tool
from agents import Agent, ModelSettings, Runner, function_tool, handoff, AgentHooks, RunContextWrapper, trace, Tool
from timescaledb_tools import _get_latest_indicators_multi
from agents.extensions.handoff_prompt import RECOMMENDED_PROMPT_PREFIX
from tools import get_orderbook_snapshot, get_derivatives_metrics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("OmniTrader")




# ──────────────────────────────────────────────────────────────────────────────
#  Pydantic Models for Agent Inputs/Outputs
# ──────────────────────────────────────────────────────────────────────────────
class Scenario(BaseModel):
    prob: float
    pnL_pct: float

class Scenarios(BaseModel):
    bull_case: Scenario
    bear_case: Scenario
    sideways_case: Scenario
    model_config = ConfigDict(extra='forbid')

class TechnicalAnalystOutput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    signal: Literal['BUY', 'SELL', 'HOLD']
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    confidence: int
    ev: float
    regime: Literal['trend', 'range', 'volatile', 'squeeze']
    scenarios: Scenarios
    rationale: str

class ExecutionAgentInput(TechnicalAnalystOutput): # ExecutionAgent's primary data input
    pass

class ExecutionAgentConfirmation(BaseModel):
    model_config = ConfigDict(extra='forbid')

    action: str # 'buy', 'sell', 'ignored', 'wait', 'error'
    order_type: Optional[str] = None # 'limit', 'market'
    symbol: Optional[str] = None
    executed_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    order_id: Optional[str] = None
    status: Optional[str] = None # e.g., 'limit_order_placed', 'market_order_placed', 'error'
    confidence: Optional[int] = None
    ev: Optional[float] = None
    reason: Optional[str] = None # For 'ignored' or 'wait'
    error: Optional[str] = None # For 'error'
    details: Optional[dict] = None # For rich error details

class ChiefTraderFailureOutput(BaseModel):
    model_config = ConfigDict(extra='forbid')

    signal: Literal["WAIT"] = "WAIT"
    entry_price: None = None
    stop_loss: None = None
    take_profit: None = None
    confidence: Literal[0] = 0
    rationale: str


# ──────────────────────────────────────────────────────────────────────────────
# Custom Agent Hooks for Logging
# ──────────────────────────────────────────────────────────────────────────────
class TradingAgentHooks(AgentHooks):
    def __init__(self, display_name: str):
        self.event_counter = 0
        self.display_name = display_name
        self.agent_logger = logging.getLogger(f"AgentHooks.{display_name}")

    async def on_start(self, context: RunContextWrapper, agent: Agent) -> None:
        self.event_counter += 1
        self.agent_logger.info(f"({self.event_counter}) Agent {agent.name} started.")

    async def on_end(self, context: RunContextWrapper, agent: Agent, output: Any) -> None:
        self.event_counter += 1
        # Avoid overly verbose logging of full output data, summarize if possible
        output_summary = str(output)[:200] + "..." if len(str(output)) > 200 else str(output)
        self.agent_logger.info(f"({self.event_counter}) Agent {agent.name} ended. Output: {output_summary}")

    async def on_handoff(self, context: RunContextWrapper, agent: Agent, source: Agent) -> None:
        self.event_counter += 1
        self.agent_logger.info(f"({self.event_counter}) Agent {source.name} handing off to {agent.name}.")

    async def on_tool_start(self, context: RunContextWrapper, agent: Agent, tool: Tool) -> None:
        self.event_counter += 1
        self.agent_logger.info(f"({self.event_counter}) Agent {agent.name} started tool {tool.name}.")

    async def on_tool_end(
        self, context: RunContextWrapper, agent: Agent, tool: Tool, result: str
    ) -> None:
        self.event_counter += 1
        # Avoid overly verbose logging of full tool result data
        result_str = str(result)
        result_summary = result_str[:200] + "..." if len(result_str) > 200 else result_str
        self.agent_logger.info(f"({self.event_counter}) Agent {agent.name} ended tool {tool.name}. Result: {result_summary}")


# ──────────────────────────────────────────────────────────────────────────────
# Optional data tools (always return dict, never raise)
# ──────────────────────────────────────────────────────────────────────────────
# Removed orderbook_snapshot and derivatives_metrics wrappers (unused)

# --- Execution function tool for ExecutionAgent ---
@function_tool
def execute_limit_order_on_bybit(
    side: str,
    entry: float,
    stop_loss: float,
    take_profit: float,
    post_only: bool
) -> dict:
    """
    Robust LIMIT order creator with native TP/SL on Bybit USDT-Perps.
    – Rejects immediately if the exchange rejects.
    – Optionally sets PostOnly only when price is on the passive side.
    """
    import os, time, ccxt, math
    symbol = "ETH/USDT:USDT"
    qty     = float(os.getenv("ORDER_SIZE", 1))

    # Default for post_only if not provided
    if post_only is None:
        post_only = False

    ex = ccxt.bybit({
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_API_SECRET"),
        "enableRateLimit": True,
        "options": {"defaultType": "linear"}
    })

    # Decide if a post-only flag makes sense for this price
    ob  = ex.fetch_order_book(symbol, 1)
    best_bid, best_ask = ob["bids"][0][0], ob["asks"][0][0]
    would_cross = (side.lower() == "buy"  and entry >= best_ask) or \
                  (side.lower() == "sell" and entry <= best_bid)
    params = {
        "timeInForce": "PostOnly" if post_only and not would_cross else "GoodTillCancel",
        "takeProfit":  take_profit,
        "stopLoss":    stop_loss,
        "tpTriggerBy": "LastPrice",
        "slTriggerBy": "LastPrice",
        "tpslMode":    "Full"
    }

    try:
        order = ex.create_order(symbol, "limit", side, qty, entry, params)
        order_id = order.get("id") or order.get("orderId") or order.get("info", {}).get("orderId")

        if not order_id:
            return {
                "status": "error",
                "error": "No order id returned by exchange.",
                "exchange_msg": order.get("info"),
                "parameters": {"side": side, "entry": entry,
                               "stop_loss": stop_loss, "take_profit": take_profit},
                "symbol": symbol, "order_size": qty,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

        # If order_id exists, treat as placed, regardless of status
        return {
            "status": "limit_order_placed",
            "entry_order_id": order_id,
            "entry_order_response": order,
            "parameters": {"side": side, "entry": entry,
                           "stop_loss": stop_loss, "take_profit": take_profit},
            "symbol": symbol, "order_size": qty,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

    except Exception as e:
        return {
            "status": "error",
            "error":  str(e),
            "parameters": {"side": side, "entry": entry,
                           "stop_loss": stop_loss, "take_profit": take_profit},
            "symbol": symbol, "order_size": qty,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }


@function_tool
def execute_market_order_on_bybit(side: str, stop_loss: float, take_profit: float) -> dict:
    """
    Places a MARKET order with TP2 and SL on Bybit USDT Perpetuals using native bracket order logic.
    Args:
        side: "buy" or "sell"
        stop_loss: stop loss price (float)
        take_profit: take profit price (float)
    Returns: dict with order response, IDs, parameters, and status or error.
    """
    import os, time
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")
    symbol = "ETH/USDT:USDT"
    order_size = float(os.getenv("ORDER_SIZE", 1))
    cfg = {
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {"defaultType": "linear"}
    }
    try:
        import ccxt
        ex = ccxt.bybit(cfg)
        order_params = {
            "takeProfit": take_profit,
            "stopLoss": stop_loss,
            "tpTriggerBy": "LastPrice",
            "slTriggerBy": "LastPrice",
            "tpslMode": "Full"
        }
        entry_order = ex.create_order(symbol, "market", side, order_size, None, order_params)
        return {
            "status": "market_order_placed",
            "entry_order_id": entry_order["id"],
            "entry_order_response": entry_order,
            "parameters": {"side": side, "stop_loss": stop_loss, "take_profit": take_profit},
            "symbol": symbol,
            "order_size": order_size,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e),
            "parameters": {"side": side, "stop_loss": stop_loss, "take_profit": take_profit},
            "symbol": symbol,
            "order_size": order_size,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

@function_tool
def get_current_price(symbol: str) -> float:
    """
    Fetch the latest price for the given symbol from Bybit USDT Perpetuals using ccxt.
    Returns the current mark price as float.
    """
    import ccxt
    import os
    symbol = symbol or os.getenv("SYMBOL", "ETH/USDT:USDT")
    exchange = ccxt.bybit({
        "enableRateLimit": True,
        "options": {"defaultType": "linear"},
        "apiKey": os.getenv("BYBIT_API_KEY"),
        "secret": os.getenv("BYBIT_API_SECRET"),
    })
    ticker = exchange.fetch_ticker(symbol)
    return float(ticker["last"])


      # ──────────────────────────────────────────────────────────────────────────────
#  ONE‑SHOT DATA TOOL  →  replaces 3 separate calls
# ──────────────────────────────────────────────────────────────────────────────
@function_tool
def get_market_context(symbol: str, timeframes: list[str]) -> dict:
    """
    Fetches all required market context in a single round‑trip:
      • multi‑TF indicators (for all timeframes given) for the target symbol (e.g. ETH/USDT:USDT)
      • BTC context: per-timeframe *summary* objects from the BTC ETL pipeline (key=value string of all BTC metrics; raw numbers stripped – see etl_btc_context.py)
      • full order‑book snapshot
      • derivatives / funding metrics
      • indicator_window: last 30 bars for each timeframe via ``get_last_n_indicators``
    Always returns a dict; embeds error messages if any sub‑call fails.
    BTC context is provided under the 'btc_context' key as:
      {'btc_context': { timeframe: { 'timestamp': int, 'summary': str } }}
    """
    if not symbol:
        symbol = "ETH/USDT:USDT"
    if not timeframes:
        timeframes = ["1m", "5m", "15m", "1h", "4h", "1d"]

    ctx = {"symbol": symbol}
    try:
        ctx["indicators"] = _get_latest_indicators_multi(symbol, timeframes)
    except Exception as e:
        ctx["indicators_error"] = str(e)

    # --- Historical BB width series for BTC/ETH, all TFs ---
    try:
        import timescaledb_tools
        # Only include indicator_window (no bb_series, no vol_state)
        win = {}
        window_size = int(os.getenv("INDICATOR_WINDOW_SIZE", 5))
        for tf in timeframes:
            win[tf] = timescaledb_tools.get_last_n_indicators_py(symbol, tf, window_size)
        ctx["indicator_window"] = win
    except Exception as e:
        ctx["history_series_error"] = str(e)


    # --- BTC context: multi-TF, cross-asset, volatility, etc ---
    try:
        btc_tf = timeframes
        btc_symbol = "BTC/USDT:USDT"
        btc_context = _get_latest_indicators_multi(btc_symbol, btc_tf)
        btc_context_summary = {}
        for tf, data in btc_context.items():
            btc_context_summary[tf] = {
                "timestamp": data["timestamp"],
                "summary": "&".join(f"{k}={v}" for k, v in data.items() if k != "timestamp")
            }
        ctx["btc_context"] = btc_context_summary
    except Exception as e:
        ctx["btc_context_error"] = str(e)

    try:
        ctx["orderbook"] = get_orderbook_snapshot(symbol)
    except Exception as e:
        ctx["orderbook_error"] = str(e)

    try:
        ctx["derivatives"] = get_derivatives_metrics(symbol)
    except Exception as e:
        ctx["derivatives_error"] = str(e)

    return ctx



execution_agent = Agent(
    name="ExecutionAgent",
    handoff_description="Executes trading signals when handed off.",

     instructions=r"""
###############################################################################
#  ExecutionAgent v2 · ETH/USDT (Bybit)
#  ROLE: You are ExecutionAgent, responsible for executing validated trade signals
#  with precise adherence to risk and execution constraints.
#  CRITICAL GLOBAL RULE: Strictly adhere to input validations and execution logic.
#  CRITICAL OUTPUT RULE: Your final output MUST strictly conform to ExecutionAgentConfirmation schema.
###############################################################################

### 0 · CONTEXT & INPUT VALIDATION (MANDATORY FIRST STEP)
data = payload.get("input", payload)
if isinstance(data, str):
    data = json.loads(data)

# Validate input schema: {signal, entry_price, stop_loss, take_profit, confidence, ev, scenarios}
required_keys = ["signal", "entry_price", "stop_loss", "take_profit", "confidence", "ev", "scenarios"]
for key in required_keys:
    if key not in data:
        OUTPUT = {"action":"error","error":f"Missing required key: {key}"}
        STOP EXECUTION IMMEDIATELY

### STAGE_0_DONE: Input Validated. PROCEED TO STAGE 1.

### 1 · CONFIDENCE & SIGNAL TYPE FILTER (CRITICAL FILTERING)
if data["confidence"] < 45:
    reason = data.get("rationale", "Low confidence, trade not executed.")
    OUTPUT = {"action":"ignored","reason": reason, "confidence": data["confidence"], "ev": data["ev"], "symbol":"ETH/USDT:USDT"}
    STOP EXECUTION IMMEDIATELY

elif data["signal"] == "HOLD":
    # Pass through all TA fields for transparency
    output = {
        "action": "hold",
        "order_type": None,
        "symbol": "ETH/USDT:USDT",
        "executed_price": None,
        "stop_loss": data.get("stop_loss"),
        "take_profit": data.get("take_profit"),
        "order_id": None,
        "status": None,
        "confidence": data.get("confidence"),
        "ev": data.get("ev"),
        "rationale": data.get("rationale", "Explicit HOLD from TechnicalAnalyst."),
        "error": None,
        "details": None
    }
    # Include extra fields if present
    for field in ["regime", "scenarios", "entry_price"]:
        if field in data:
            output[field] = data[field]
    OUTPUT = output
    STOP EXECUTION IMMEDIATELY

### STAGE_1_DONE: Signal & Confidence Filtered. PROCEED TO STAGE 2.

### 2 · CURRENT PRICE FETCH & ORDER TYPE DECISION
current_price = get_current_price("ETH/USDT:USDT")

if data["signal"] == "BUY":
    if data["entry_price"] is None:
        OUTPUT = {"action":"error","error":"Null entry_price for BUY signal"}
        STOP EXECUTION IMMEDIATELY
    order_type = "market" if current_price <= data["entry_price"] else "limit"

elif data["signal"] == "SELL":
    if data["entry_price"] is None:
        OUTPUT = {"action":"error","error":"Null entry_price for SELL signal"}
        STOP EXECUTION IMMEDIATELY
    order_type = "market" if current_price >= data["entry_price"] else "limit"

else:
    OUTPUT = {"action":"error","error":"Invalid signal provided."}
    STOP EXECUTION IMMEDIATELY

### STAGE_2_DONE: Order Type Determined ({order_type.upper()}). PROCEED TO STAGE 3.

### 3 · ORDER EXECUTION (MANDATORY)
if order_type == "market":
    resp = execute_market_order_on_bybit(
        side=data["signal"].lower(),
        stop_loss=data["stop_loss"],
        take_profit=data["take_profit"]
    )
else: # Limit order
    resp = execute_limit_order_on_bybit(
        side=data["signal"].lower(),
        entry=data["entry_price"],
        stop_loss=data["stop_loss"],
        take_profit=data["take_profit"],
        post_only=False
    )

if resp.get("status") == "error":
    OUTPUT = {"action":data["signal"].lower(),"status":"error","error":resp.get("error", "Execution error"), "details": resp, "confidence":data["confidence"],"ev":data["ev"],"symbol":"ETH/USDT:USDT"}
    STOP EXECUTION IMMEDIATELY

### STAGE_3_DONE: Order Executed Successfully. PROCEED TO STAGE 4.

### 4 · FINAL OUTPUT GENERATION (STRICT JSON SCHEMA)
OUTPUT = {
    "action": data["signal"].lower(),
    "order_type": order_type,
    "symbol": "ETH/USDT:USDT",
    "executed_price": resp.get("entry_order_response", {}).get("average", current_price) if order_type == "market" else data["entry_price"],
    "stop_loss": data["stop_loss"],
    "take_profit": data["take_profit"],
    "order_id": resp.get("entry_order_id"),
    "status": resp.get("status", "unknown"),
    "confidence": data["confidence"],
    "ev": data["ev"],
    "reason": data.get("rationale", None)
}

###############################################################################
# END PROMPT – All instructions rigorously followed. Output is pure JSON.
###############################################################################
""",
    tools=[get_current_price, execute_limit_order_on_bybit, execute_market_order_on_bybit],
    output_type=ExecutionAgentConfirmation,
    model="gpt-4.1", # Consider a more capable model if complex JSON handling is needed
    hooks=TradingAgentHooks(display_name="ExecutionAgent") # Added hooks
)




def on_ta_handoff(ctx):
    logger.info("[Handoff] TechnicalAnalyst handing off to ExecutionAgent.")

from agents.extensions import handoff_filters

# ──────────────────────────────────────────────────────────────────────────────
# Prompt factory
# ──────────────────────────────────────────────────────────────────────────────


def technical_analyst_instructions(context, agent=None):
    """
    Returns the Omni-TA v7 prompt string.

    Args:
        context:   RunContextWrapper exposing TradingContext.
        agent:     (unused, but kept for Agent SDK signature).
    """
    ctx = context.context                                      # unwrap TradingContext

    last_signal           = getattr(ctx, "last_signal", None)
    last_position_summary = getattr(ctx, "last_position_summary", None)

    last_signal_str           = json.dumps(last_signal, indent=2)           if last_signal else "None"
    last_position_summary_str = json.dumps(last_position_summary, indent=2) if last_position_summary else "None"

    # f-string; double braces {{ }} to render literals inside prompt
    return f"""
You are Omni-TA v9 plus — a self-directed quantitative oracle.  
Your sole task is to fuse all available information into one probability-weighted trade verdict for the current symbol.

Context
• Last Signal: {last_signal_str}  
• Last Position: {last_position_summary_str}

Mission — four mandatory steps
1. Code Interpreter: the first executable line **must** be  
      ctx_json = get_market_context()  
   (Work exclusively with that payload; no fabricated data.)

2. Engineer an edge  
   • Build whatever model(s) you judge best: Bayesian nets, tree ensembles, logistic regression, clustering + Markov chains, regime-switching vol models, etc.  
   • Start by creating a continuous evidence score and transform it into
     probabilities with a **soft-max or calibrated logistic** conversion
     (no hard-coded rule tables).  
   • Feature engineering, back-tests, Monte-Carlo scenarios, risk metrics — all inside python.
   • Use the look-back data to capture near-term momentum and broader regime shifts.
   • Decide whether the setup is an intraday scalp, a swing trade or something else, and tailor EV calculations to that horizon.
   • Seek an optimal entry zone (prefer limit orders or pullbacks) before committing to market entry.
   • Seed any stochastic operations (e.g., `np.random.seed(42)`) to keep results reproducible across cycles.
   • Factor in `last_signal` and `last_position_summary` when designing the edge to see how previous siganl and trade performed.
   • Over-fit guards: walk-forward split, k-fold CV, AIC/BIC, or similar.  Note your safeguard briefly in the rationale.

  3. Produce the JSON object below and `print` it — **nothing else**.
   • signal ∈ {{BUY, SELL, HOLD}} (or WAIT if tools fail). Choose whichever fits the analysis and horizon.
   • Probabilities must obey:  
     – Sum ≈ 1.00 (±0.01).  
     – bull_case.prob ≥ 0.05 and bear_case.prob ≥ 0.05 **unless signal = HOLD**.  
     – sideways_case.prob ≤ 0.90.  
     – **Directional rule:**  
         BUY  → bull_prob > sideways_prob  
         SELL → bear_prob > sideways_prob  
         Otherwise → signal = HOLD (entry/SL/TP = null).  
   • confidence =  
         int( 100 * max(bull_prob, bear_prob) * abs(ev) / (abs(ev)+1) )  
     (so certainty grows with both directional conviction and EV).  
   • ev = expected % return (4 decimals).
   • Keep ATR-based risk/reward sensible (aim ≥ 2:1 unless vol is ultra-low) and plan for a limit entry price whenever possible.
   • Mention the trade horizon (scalp, swing, etc.) in the rationale.

4. Self-QA checklist (execute in python immediately before printing)  
   ✓ Probabilities satisfy all rules above and sum within 0.01 of 1.0.  
   ✓ If signal is BUY or SELL, its probability > sideways_prob.  
   ✓ Numerical fields are floats / ints, not strings.  
   ✓ rationale ≤ 650 chars; no tool output or stack traces.  
   If any check fails, fix and re-generate before printing.

Available Data via get_market_context
• Multi-TF indicators (1 m … 1 d): RSI, ADX, MA slopes, VWAP dev, ATR, BB width, OBV, etc.
• 30-bar indicator history for each timeframe (``indicator_window``)
• Cross-asset context: BTC, ETH correlations & spreads.
• Order-book: imbalance, depth curve, micro-price, liquidity lambda, spread.
• Derivatives: funding, OI deltas/Z-scores, long/short skew, liquidations.
• Volatility regimes & realised-vol history.

Output schema
```json
{{
  "signal": "BUY" | "SELL" | "HOLD",
  "entry_price": float | null,
  "stop_loss": float | null,
  "take_profit": float | null,
  "confidence": integer,            // 0-100
  "ev": float,                      // 4-dec pct
  "regime": "trend" | "range" | "volatile" | "squeeze",
  "scenarios": {{
    "bull_case":     {{ "prob": float, "pnL_pct": float }},
    "bear_case":     {{ "prob": float, "pnL_pct": float }},
    "sideways_case": {{ "prob": float, "pnL_pct": float }}
  }},
  "rationale": "Data-driven reasoning, ≤650 chars."
}}
Guiding ethos
• No fixed thresholds — today’s data decide today’s edge.
• Sideways probability may rise when evidence conflicts, but never reach certainty.
• Every number must trace back to an actual computation.
• Print the JSON, return it verbatim, call the handoff tool. Nothing more.
• Always strive for the best possible entry price rather than blindly jumping in.

Think freely → Validate quantitatively → Self-QA → Emit JSON → Handoff.
"""


# ──────────────────────────────────────────────────────────────────────────────
# Agent definition
# ──────────────────────────────────────────────────────────────────────────────
from agents import CodeInterpreterTool

technical_analyst = Agent(
    name="TechnicalAnalyst",
    handoff_description="Analyses data and hands off a trade idea.",
    handoffs=[handoff(execution_agent, on_handoff=on_ta_handoff, input_filter=handoff_filters.remove_all_tools)],
    output_type=TechnicalAnalystOutput,
    instructions=technical_analyst_instructions,
   tools=[
        get_market_context,
        CodeInterpreterTool(tool_config={"type": "code_interpreter", "container": {"type": "auto"}})
    ],
    model="gpt-4.1", # Kept gpt-4.1 as it needs to produce complex analysis
    hooks=TradingAgentHooks(display_name="TechnicalAnalyst") # Added hooks
)

# ──────────────────────────────────────────────────────────────────────────────
#  Coordination agent – new structured tool rules + fallback JSON
# ──────────────────────────────────────────────────────────────────────────────
from typing import Union

# Strict schema models for ChiefTrader output
from typing import Literal

class Scenario(BaseModel):
    prob: float
    pnL_pct: float
    model_config = ConfigDict(extra='forbid')

class Scenarios(BaseModel):
    bull_case: Scenario
    bear_case: Scenario
    sideways_case: Scenario
    model_config = ConfigDict(extra='forbid')

class ChiefTraderOutput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    signal: Literal["BUY", "SELL", "HOLD", "WAIT"]
    entry_price: Optional[float] = None
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    confidence: int
    rationale: str
    regime: Optional[Literal["trend", "range", "volatile", "squeeze"]] = None
    scenarios: Optional[Scenarios] = None
    ev: Optional[float] = None

# ChiefTrader: Handoffs to TechnicalAnalyst for signal generation
chief_trader = Agent(
    name="ChiefTrader",
    instructions=f"""{RECOMMENDED_PROMPT_PREFIX}
STRICT OUTPUT: raw JSON only (no markdown).
Think silently; do NOT reveal chain-of-thought.

### CONTEXT
• The previous signal is available as `context.last_signal` (a dict or None).
• REQUEST_SIGNAL: string with symbol, timeframes, extras.

### TASK
Invoke *TechnicalAnalyst* exactly once to generate a fresh trading signal (which should conform to TechnicalAnalystOutput schema).
The handoff to *TechnicalAnalyst* will result in a TechnicalAnalystOutput schema JSON.

### FAILURE HANDLING
If any mandatory tool errors occur within *TechnicalAnalyst* (indicators, orderbook_snapshot, derivatives_metrics), *TechnicalAnalyst* itself is instructed to output a specific WAIT signal.
If *TechnicalAnalyst* fails to produce valid JSON or has other unrecoverable errors, this agent should ensure a fallback.
If the handoff to *TechnicalAnalyst* ultimately results in an error or invalid output,
immediately OUTPUT a JSON object conforming to the ChiefTraderFailureOutput schema:

Example:
{{
  "signal":       "WAIT",
  "entry_price":  null,
  "stop_loss":    null,
  "take_profit":  null,
  "confidence":   0,
  "rationale":    "tool failure: <tool_name_or_reason>"
}}
Ensure the 'rationale' key clearly states the problem, e.g., "TechnicalAnalyst failed to return valid JSON" or "TechnicalAnalyst reported tool failure: orderbook data missing".
""",
    handoffs=[technical_analyst],
    output_type=ChiefTraderOutput,
    model="gpt-4.1",
    hooks=TradingAgentHooks(display_name="ChiefTrader")
)
# ──────────────────────────────────────────────────────────────────────────────
#  Position check helper (includes pending limit order check)
# ──────────────────────────────────────────────────────────────────────────────
def get_open_position_summary(symbol="ETH/USDT:USDT") -> tuple[bool, dict|None]:
    """
    Checks for open positions and open orders. Returns (has_open, summary_dict or None).
    summary_dict contains details of the open position/order if found.
    """
    cfg = {"apiKey": os.getenv("BYBIT_API_KEY"),
           "secret": os.getenv("BYBIT_API_SECRET"),
           "enableRateLimit": True,
           "options": {"defaultType": "linear"}}
    ex = None
    try:
        import ccxt
        ex = ccxt.bybit(cfg)
    except Exception as e:
        logger.error(f"[Bybit Init] Failed to initialize Bybit: {e}")
        return False, {"error": str(e)}

    # Try positions
    positions = None
    try:
        positions = ex.fetch_positions([symbol])
        open_positions = [p for p in positions if abs(float(p.get("contracts", p.get("size", 0)))) > 0]
        if open_positions:
            logger.info(f"Open position detected for {symbol}.")
            # Return the first open position for summary
            return True, {"type": "position", "details": open_positions[0]}
    except Exception as e:
        logger.warning(f"[Bybit] fetch_positions failed: {e}")

    # Try open orders
    orders = None
    try:
        orders = ex.fetch_open_orders(symbol)
        open_orders = [o for o in orders if o.get("status", "") in ("open", "new")]
        if open_orders:
            logger.info(f"Open order detected for {symbol}.")
            # Return the first open order for summary
            return True, {"type": "order", "details": open_orders[0]}
    except Exception as e:
        logger.warning(f"[Bybit] fetch_open_orders failed: {e}")

    # If both failed
    if positions is None and orders is None:
        logger.error(f"Both fetch_positions and fetch_open_orders failed for {symbol}. Proceeding with loop.")
        return False, {"error": "Failed to fetch positions and orders."}

    return False, None

# ──────────────────────────────────────────────────────────────────────────────
#  Persistent LAST_SIGNAL  +  async polling loop
# ──────────────────────────────────────────────────────────────────────────────
INTERVAL          = 60          # seconds between cycles
TIMEOUT           = 340          # per-call budget
GRACE_RETRIES     = 1           # retry once on timeout
LAST_SIGNAL_FILE  = os.getenv("LAST_SIGNAL_FILE", "last_signal.json")

def load_last_signal(path=None):
    # Always use the env LAST_SIGNAL_FILE unless overridden
    if path is None:
        path = LAST_SIGNAL_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_last_signal(sig: dict | None) -> None:
    if sig is None:
        return
    with open(LAST_SIGNAL_FILE, "w") as f:
        json.dump(sig, f)

def extract_json_block(text):
    # Remove markdown code fences and whitespace
    text = re.sub(r"^```(json)?", "", text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r"```$", "", text, flags=re.MULTILINE)
    text = text.strip()
    # Extract the first {...} JSON object
    match = re.search(r"({.*})", text, re.DOTALL)
    if match:
        return match.group(1)
    return text

LAST_SIGNAL = load_last_signal()

def pretty_print_last_signal(path=None):
    if path is None:
        path = LAST_SIGNAL_FILE
    if not os.path.exists(path):
        print("No last signal found.")
        return
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    try:
        # Try to parse as JSON
        signal = json.loads(content)
        print("\n===== LAST SIGNAL (JSON) =====")
        print(json.dumps(signal, indent=2))
        print("==============================\n")
    except json.JSONDecodeError:
        # Plain text
        print("\n===== LAST SIGNAL (TEXT) =====")
        print(content)
        print("==============================\n")

def pretty_print_position_summary(summary):
    if not summary:
        print("No summary available.")
        return
    if summary.get("type") == "position":
        d = summary["details"]
        info = d.get("info", {})
        print("\n===== OPEN POSITION SUMMARY =====")
        print(f"Symbol:          {d.get('symbol', info.get('symbol'))}")
        print(f"Side:            {info.get('side', d.get('side'))}")
        print(f"Size:            {info.get('size', d.get('contracts'))}")
        print(f"Leverage:        {info.get('leverage', d.get('leverage'))}")
        print(f"Entry Price:     {info.get('avgPrice', d.get('entryPrice'))}")
        print(f"Mark Price:      {info.get('markPrice', d.get('markPrice'))}")
        print(f"Liq. Price:      {info.get('liqPrice', d.get('liquidationPrice'))}")
        print(f"Unrealized PnL:  {info.get('unrealisedPnl', d.get('unrealizedPnl'))}")
        print(f"Stop Loss:       {info.get('stopLoss', d.get('stopLossPrice'))}")
        print(f"Take Profit:     {info.get('takeProfit', d.get('takeProfitPrice'))}")
        print("==================================\n")
    # Do not print anything for open orders or other types

from pydantic import BaseModel

class TradingContext(BaseModel):
    last_signal: dict | None = None
    last_position_summary: dict | None = None  # Persisted last position/order summary with status
    # Add more shared state/config as needed

POSITION_SUMMARY_FILE = os.getenv("POSITION_SUMMARY_FILE", "position_summary.json")

def load_position_summary(path=None):
    if path is None:
        path = POSITION_SUMMARY_FILE
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def extract_position_summary(summary):
    """
    Given the raw summary dict from get_open_position_summary,
    extract a concise summary dict with only the fields of interest.
    """
    if not summary or summary.get("type") != "position":
        return None
    d = summary["details"]
    info = d.get("info", {})
    return {
        "symbol": d.get("symbol", info.get("symbol")),
        "side": info.get("side", d.get("side")),
        "size": info.get("size", d.get("contracts")),
        "leverage": info.get("leverage", d.get("leverage")),
        "entry_price": info.get("avgPrice", d.get("entryPrice")),
        "mark_price": info.get("markPrice", d.get("markPrice")),
        "liq_price": info.get("liqPrice", d.get("liquidationPrice")),
        "unrealized_pnl": info.get("unrealisedPnl", d.get("unrealizedPnl")),
        "stop_loss": info.get("stopLoss", d.get("stopLossPrice")),
        "take_profit": info.get("takeProfit", d.get("takeProfitPrice")),
    }

def save_position_summary(summary: dict | None) -> None:
    if summary is None:
        with open(POSITION_SUMMARY_FILE, "w") as f:
            json.dump(None, f)
        return
    with open(POSITION_SUMMARY_FILE, "w") as f:
        json.dump(summary, f)

# Track previous open state at module level
PREVIOUSLY_OPEN = False
LAST_OPEN_SUMMARY = None

async def one_cycle():
    global LAST_SIGNAL, PREVIOUSLY_OPEN, LAST_OPEN_SUMMARY

    has_open, summary = get_open_position_summary()
    if has_open:
        logger.info("In position or open order – skipping.")
        if summary:
            pretty_print_position_summary(summary)
            concise_summary = extract_position_summary(summary)
            if concise_summary is not None:
                concise_summary["status"] = "open"
                save_position_summary(concise_summary)
                LAST_OPEN_SUMMARY = concise_summary.copy()
        PREVIOUSLY_OPEN = True
        # Load for context
        last_position_summary = load_position_summary()
        # Block signal generation: skip agent chain, but do not update or save LAST_SIGNAL
        return
    else:
        if PREVIOUSLY_OPEN and LAST_OPEN_SUMMARY:
            # Just closed: mark last open summary as closed
            closed_summary = LAST_OPEN_SUMMARY.copy()
            closed_summary["status"] = "closed"
            save_position_summary(closed_summary)
        else:
            # No open and nothing just closed: keep the last summary
            last_position_summary = load_position_summary()
        PREVIOUSLY_OPEN = False

    # Prepare agent-driven input and context
    input_items = []
    context = TradingContext(
        last_signal=LAST_SIGNAL,
        last_position_summary=last_position_summary
    )
    user_input = (
        "REQUEST_SIGNAL:symbol=ETH/USDT:USDT tfs=[1m,5m,15m,1h,4h,1d] "
        "extras=orderbook,derivatives return=json"
    )
    input_items.append({"content": user_input, "role": "user"})

    cycle_trace_id = f"trading_cycle_{int(time.time())}"
    trace_metadata = {
        'run_type': 'live',
        'symbol': 'ETH/USDT:USDT',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
    }
    with trace("OmniTrader_One_Cycle", group_id=cycle_trace_id):
        current_agent = chief_trader
        try:
            result = await Runner.run(current_agent, input_items, context=context)
            for new_item in result.new_items:
                agent_name = new_item.agent.name
                if hasattr(new_item, 'content'):
                    logger.info(f"{agent_name}: {getattr(new_item, 'content', '')}")
                elif hasattr(new_item, 'output'):
                    logger.info(f"{agent_name}: Tool call output: {getattr(new_item, 'output', '')}")
                elif hasattr(new_item, 'source_agent') and hasattr(new_item, 'target_agent'):
                    logger.info(f"Handed off from {new_item.source_agent.name} to {new_item.target_agent.name}")
                else:
                    logger.info(f"{agent_name}: Skipping item: {new_item.__class__.__name__}")
            input_items = result.to_input_list()
            current_agent = result.last_agent

            # Save the final output
            final_output_data = None
            if result.final_output:
                if isinstance(result.final_output, (dict, BaseModel)):
                    final_output_data = result.final_output
                else:
                    try:
                        final_output_data = json.loads(result.final_output)
                    except Exception:
                        final_output_data = {"raw_output": str(result.final_output)}
            if final_output_data:
                if isinstance(final_output_data, BaseModel):
                    LAST_SIGNAL = final_output_data.model_dump()
                elif isinstance(final_output_data, dict):
                    LAST_SIGNAL = final_output_data
                else:
                    LAST_SIGNAL = {"raw_output": str(final_output_data)}
                save_last_signal(LAST_SIGNAL)
                logger.info("Final agent chain output:\n%s", json.dumps(LAST_SIGNAL, indent=2))
                pretty_print_last_signal(LAST_SIGNAL_FILE)
            else:
                logger.warning("Agent chain returned no final_output.")
                LAST_SIGNAL = {"signal": "WAIT", "rationale": "Agent chain returned no output"}
                save_last_signal(LAST_SIGNAL)
        except asyncio.TimeoutError:
            logger.error("LLM timeout; skipping cycle.")
            LAST_SIGNAL = {"signal": "WAIT", "rationale": "LLM timeout"}
            save_last_signal(LAST_SIGNAL)
        except Exception as e:
            logger.error(f"Unhandled error in one_cycle: {e}", exc_info=True)
            LAST_SIGNAL = {"signal": "WAIT", "rationale": f"Unhandled error: {str(e)}"}
            save_last_signal(LAST_SIGNAL)
            return

async def loop():
    while True:
        await one_cycle()
        await asyncio.sleep(INTERVAL)

if __name__ == "__main__":
    asyncio.run(loop())
