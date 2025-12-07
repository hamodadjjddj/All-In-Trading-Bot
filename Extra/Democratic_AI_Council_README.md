# Democratic AI Council — GoldTradingBot
**Comprehensive manual / README**  
_This document is designed as a detailed, step-by-step manual you can read on your phone. It explains the architecture, data sources, agent prompts, orchestration, technical module, hallucination mitigation, and practical options to run large 70B models in the cloud (including low-cost/free paths)._

---

## Table of contents
1. Overview & Goals
2. High-level architecture
3. Data collection (detailed)
4. Technical module (calculations)
5. Agent design: roles, prompts, and outputs
6. Two-phase decision flow: Context + Plan
7. Debate, voting, and aggregation rules
8. Trade execution and position sizing (including total balance)
9. Hallucination mitigation (practical recipes)
10. Running 70B LLMs: options, costs, and free/credit paths
11. Implementation notes: orchestration code snippets
12. Security, privacy, and operational cautions
13. Appendix: example JSON schemas, prompts, and test vectors
14. Change log & further reading

---

## 1) Overview & Goals
You want a **Democratic AI Council** that helps decide trades for gold.  
The system should:
- Collect fundamentals, OHLCV, sentiment, and technical indicators in the order: last **7 days → 24 hours → 4 hours** for each category.
- Feed **the same full dataset** to multiple LLM agents.
- Each agent outputs a **market context reading** (Bullish / Bearish / Uncertain).
- If majority agrees on the context, agents propose **trading plans** (entry, TP, SL, pip-based targets).
- The agents debate and vote; the winning plan is executed.
- A technical module computes objective SR/RSI/MACD/EMA and is supplied to agents (not an agent itself).
- The system reports **total account balance** and uses it to size positions and tolerance-level entry ladders (quick small moves, not unrealistic 100-pip targets).

---

## 2) High-level architecture
- **Collector**: Fetches fundamentals, news, ETF/CB purchases, DXY, OHLCV windows, and sentiment for 7d/24h/4h.
- **Technical Module**: Computes support/resistance (7d/24h/4h), RSI, MACD, EMAs, ATR.
- **Agents**: 3–6 LLMs with different personas (Macro, Sentiment, Technician, Risk).
- **Coordinator / Orchestrator**: Handles rounds: context vote → if majority: plan proposals → debate → final vote.
- **Execution Engine**: Position sizing, order placement, and logging.
- **Audit & Safety**: human-in-the-loop flags, replay logs, and backtesting hooks.

---

## 3) Data collection (detailed)
### A) Fundamentals (7d, 24h, 4h)
- CPI/PCE, Fed statements, FedFunds futures changes, real rates (10y - CPI), unemployment updates.
- Central bank net purchases (weekly/monthly snapshots) and SPDR Gold Shares flows.
- DXY snapshots and moving averages for same windows.

**Practical sources**:
- FRED (for rates), BLS (for CPI), World Gold Council (central bank purchases), SPDR/ETF flows, Investing/Yahoo for DXY & gold price.

### B) News & Events (ordered)
Collect news in this order for gold:
1. Last 7 days → context-setting events (central bank announcements, CPI prints).
2. Last 24 hours → high-relevance events (breaking Fed commentary, big ETF flows).
3. Last 4 hours → immediate catalysts (unexpected headlines, short-term liquidity).

For each article store: timestamp, title, snippet, URL, sentiment score, named entities (Fed, ECB, CPI, 'gold', 'ETF', 'bank name').

### C) OHLCV & Market microstructure (ordered)
Pull OHLCV for the instrument you trade (XAUUSD or GLD) with these windows:
- **7d**: daily or hourly aggregation for trend detection
- **24h**: 1H or 15m aggregation for intraday moves
- **4h**: 5m or 1m depending on your execution speed (use 5m as default)

Also store recent trade volume spikes, ATR (14), and realized vol.

### D) Sentiment (social + public)
- Pull from Twitter/X (filtered to keywords), Reddit (r/gold, r/investing), Google Trends, and any commercial sentiment API you have.
- Produce aggregated sentiment: `sentiment_score` (-1..+1), top bullish/bearish keywords, bucket by timeframe (7d/24h/4h).

---

## 4) Technical module (calculations)
This is deterministic code you run once per data refresh.

- **SR (Support/Resistance)**:
  - 7d S/R: daily high & low over 7 days; also pivot points.
  - 24h S/R: high/low over 24h, intraday pivots.
  - 4h S/R: short-term high/low.

- **Indicators**:
  - EMA(20), EMA(50), EMA(200)
  - RSI(14)
  - MACD (12,26,9)
  - ATR(14)
  - Bollinger Bands (20,2)

- **Volatility-based SL/TP suggestions**:
  - Use ATR: SL = entry - k * ATR (k=1.0..2.5 depending on risk tolerance).
  - Quick-targets: TP1 = entry + 0.5 * ATR, TP2 = entry + 1.0 * ATR (for quick small wins).

---

## 5) Agent design: roles, prompts, outputs
All agents get the **same JSON** object with fields `fundamentals`, `news`, `ohlcv`, `technicals`, `sentiment`, `balance`.

### Suggested agents:
- **Macro Analyst**: Focus on CPI, rates, Fed tone, CB purchases.
- **Sentiment Analyst**: Social + news sentiment, retail flow signals.
- **Technician**: Price only; uses SR, RSI, EMA to propose mechanical entries.
- **Risk Manager**: Focus on position sizing, portfolio drawdown, stop placement.

### Phase 1 prompt (Context read)
```
You are {ROLE}.
Given the data JSON (fundamentals, news, ohlcv, technicals, sentiment, balance),
1) Output market context: one of {Bullish | Bearish | Uncertain}
2) Provide 3 bullet points, each tied to specific data entries (cite the key numeric fields)
```
*(Note: you asked to **remove confidence score** — do not include numeric confidence fields.)*

### Phase 2 prompt (Trading plan)
Only executed if majority consensus on context:
```
You are {ROLE}. Based on the shared data and the agreed market context, propose a trading plan:
- Decision: BUY / SELL / HOLD
- Entry level (price or range)
- Position sizing (percentage of total balance)
- TP1, TP2 (in pips or absolute price)
- SL (price)
- Rationale: 3–6 bullets referencing data and technical SR
- Tolerance plan: if price misses entry, give ladder/limit order strategy (e.g., stagger entries at -0.2% / -0.5%)
```
*Do not output confidence numbers.*

---

## 6) Two-phase decision flow (automation)
1. Collector refreshes data.
2. Technical module computes indicators.
3. Send data to all agents (simultaneous).
4. Gather contexts; take majority. If no majority:
   - Run a short debate round: each agent lists why it disagreed, then re-vote (max 2 rounds).
   - If still no majority, escalate to human-in-loop or default to Hold.
5. If majority achieved: run trading plan prompts.
6. Gather trade plans, run debate/vote on plans (single-winner). Criteria: alignment to data, risk management, R:R, realism.
7. Execute the winning plan via the Execution Engine (or simulate if `dry_run=True`).

---

## 7) Debate, voting, and aggregation rules
- **Context majority**: >50% of agents (e.g., 3 of 5). Ties trigger debate round.
- **Plan voting**: use a scoring rubric computed by a voting agent or small LLM:
  - Data alignment (0–5)
  - Risk controls (0–5)
  - R:R (0–5)
  - Realism / Quickness (0–5)
  - Total score -> highest wins. In tie, prefer more conservative plan.
- **Weights**: by default Macro=0.4, Technician=0.3, Sentiment=0.2, Risk=0.1 when breaking ties.

---

## 8) Trade execution and position sizing
- Track `total_balance` in the system; each plan must set `position_pct` (e.g., 0.5% - 2% for quick intraday).
- Convert `position_pct` to lot size or units given instrument and leverage.
- Stagger entries (tolerance):
  - Example ladder for quick entry: if entry planned at 2405.50:
    - Step 1: limit 50% at 2405.50
    - Step 2: limit 30% at 2404.70 (-8 pips)
    - Step 3: limit 20% at 2403.60 (-19 pips)
  - Set SL based on ATR or nearest S/R (not a fixed pip value).
- Keep TP small for quick trades: aim for TP ~ 0.5–2 * ATR (quick scalps rather than 100 pips).

---

## 9) Hallucination mitigation (practical recipes)
Use multiple layers:
1. **Retrieval-Augmented Generation (RAG)**: Agents can only answer from the provided JSON. Implement prompts that force citation of exact fields (e.g., "Quote the `fundamentals.cpi.last` value you used").
2. **Strict output schema**: Validate agent outputs against JSON schema (reject or flag any hallucinated fields).
3. **Source-checker**: For any factual claim (e.g., "Fed hiked rates today"), run a quick automated verification subroutine against news sources you scraped and block unverifiable claims.
4. **Model ensembling + cross-checking**: Cross-compare outputs from multiple agents; if an agent's factual claim is unique and unsupported by others / data, flag for review.
5. **Fine-tune / LoRA**: If you control an open-source model, fine-tune on your domain dataset and run quantized weights to reduce creative inventing.
6. **Post-generation verification**: Use a smaller verifier model or deterministic rules to check numerical statements, ticker names, and URLs.
7. **Human-in-loop gate**: For large position sizes or unusual trades, require manual approval.

*References and practical guides include RAG best practices and Bedrock interventions.*  
(See sources cited in the chat for details.)  

---

## 10) Running 70B LLMs: options, costs, and free/credit paths
**Reality check:** a full 70B dense model requires ~140–350GB VRAM depending on precision. With quantization (4-bit GPTQ) you can run many 70B variants on GPUs with 40–80GB VRAM (A40/RTX A6000/3090/4090 combinations). Quantized variants (GPTQ/GGUF) are what people run for lower-cost inference.

**Practical low-cost/free approaches**:
- **Use quantized 70B (GPTQ) on rented GPUs**: providers like Vast.ai, RunPod, and Genesis Cloud often have cheap community or spot instances that can run GPTQ 70B on a 48–80GB GPU. (Cheapest spots sometimes ~$0.4–$2/hr depending on GPU).  
- **Hugging Face Inference Endpoints**: easier, managed, but paid by hour; may offer limited free credits. Good for production but not free long-term.  
- **Startup or academic cloud credits**: AWS, Google, Oracle, and others have startup/academic credit programs that can provide enough credits to run large instances temporarily. See available programs for eligibility.  
- **Host a trimmed model**: Use smaller high-quality models (e.g., 13B–33B) or Mixtral-8x7B which require far less VRAM but can still provide strong quality. Use many-agent ensemble to approximate larger model behavior.

**Key resources & notes**:
- Vast.ai community guides show how to deploy GPTQ 70B builds and their pricing mechanics.
- Hugging Face inference endpoints have documented hourly pricing; useful but not free.
- Cloud providers often run promotional or startup credit programs useful for temporary runs.

*Links and sources referenced in the chat: Vast.ai guides, Hugging Face pricing docs, and articles on free cloud credits and hallucination mitigation.*  

---

## 11) Implementation notes: orchestration code snippets
*(These examples are skeletons; expand and harden before production.)*

### A) JSON schema example (short)
```json
{
  "fundamentals": {...},
  "news": {"7d": [], "24h": [], "4h": []},
  "ohlcv": {"7d": {...}, "24h": {...}, "4h": {...}},
  "technicals": {"7d": {...}, "24h": {...}, "4h": {...}},
  "sentiment": {"7d":0.1,"24h":-0.2,"4h":0.05},
  "balance": 10000
}
```

### B) Orchestrator pseudocode
```python
def orchestrate_round(data_json, agents):
    # Phase 1: context
    contexts = {}
    for a in agents:
        contexts[a.name] = a.get_context(data_json)
    majority = get_majority(contexts.values())
    if not majority:
        contexts = debate_and_revoter(agents, data_json)
        majority = get_majority(contexts.values())
    if not majority:
        return {"action":"hold","reason":"no_consensus"}
    # Phase 2: plan proposals
    plans = {a.name: a.propose_plan(data_json, majority) for a in agents}
    winner = vote_on_plans(plans, data_json)
    execute_plan(winner.plan)
```

---

## 12) Security, privacy & operational cautions
- **Never** send raw secrets or user PII to third-party free gateways.
- Audit logs and immutable records are crucial (store full JSON snapshots for every decision).
- Rate-limit APIs and handle provider downtime gracefully with fallbacks (e.g., degrade to smaller models).
- Use VPN/private VPC when using cloud GPUs to avoid exposed endpoints.

---

## 13) Appendix: example prompts & test vectors
(Provided as separate files in a real repo; include here for completeness.)
- Prompt templates for Phase1 and Phase2 (copy/paste ready)
- Example data JSON with synthetic values for testing
- Unit tests: data-schema validation, agent output schema verification, plan scoring tests.

---

## 14) Change log & further reading
- v1.0 — Initial manual with full pipeline and practical guidance.
- Further reading: RAG, GPTQ quantization, Vast.ai guides, Hugging Face inference docs.

---

## Quick next-steps checklist (for immediate action)
1. Implement the collector and technical module. Start with historical OHLCV + ATR + EMA.
2. Implement strict JSON schema and a validator.
3. Implement 2–4 agent wrappers with simple prompts.
4. Wire orchestration for context vote only; use `dry_run=True`.
5. Add safety gate: require human approval for >1% balance positions.
6. Test thoroughly with backtest data and simulated agents.

---

## Files included (suggested repo)
- collector/
- technicals/
- agents/
- orchestrator/
- execution/
- tests/
- README.md (this file)

---

_End of README (truncated here to keep transmission quick). If you want, I will now generate the full, long multi-page README (expanded to many more pages of detailed step-by-step instructions, full prompt templates, full JSON schema, and reproduction steps), save it to `/mnt/data/Democratic_AI_Council_README.md`, and provide a download link._  
