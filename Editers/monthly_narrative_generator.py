#!/usr/bin/env python3
"""
Phase 2 Step 1: Monthly Digest Generator
Generates one merged monthly digest file + one prompt file (no LLM calls)
"""

import json
from pathlib import Path
from typing import List, Dict
from datetime import datetime
from dataclasses import dataclass
from collections import defaultdict

# Configuration
METADATA_PATH = Path("vector_index/metadata.jsonl")
OUTPUT_FOLDER = Path("monthly_summaries")


@dataclass
class ChunkMetadata:
    chunk_id: str
    date: str
    sources: List[str]
    original_file: str
    text: str
    token_count: int


class MonthlyDigestGenerator:
    """
    Generates monthly digest from chunks (no LLM calls)
    Output: 1 data file + 1 prompt file
    """
    
    def __init__(self, metadata_path: Path = METADATA_PATH, output_folder: Path = OUTPUT_FOLDER):
        self.metadata_path = metadata_path
        self.output_folder = output_folder
        self.output_folder.mkdir(parents=True, exist_ok=True)
        
        print("Loading chunks metadata...")
        self.chunks = self._load_metadata()
        print(f"✓ Loaded {len(self.chunks)} chunks")
    
    def _load_metadata(self) -> List[ChunkMetadata]:
        """Load all chunks from metadata file"""
        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {self.metadata_path}")
        
        chunks = []
        with open(self.metadata_path, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                chunks.append(ChunkMetadata(**data))
        return chunks
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string to datetime"""
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except:
            import re
            match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
            if match:
                return datetime.strptime(match.group(1), "%Y-%m-%d")
            raise ValueError(f"Could not parse date: {date_str}")
    
    def get_latest_month_chunks(self) -> List[ChunkMetadata]:
        """Get all chunks from the most recent month in the data"""
        # Find latest date
        latest_date = None
        for chunk in self.chunks:
            try:
                chunk_date = self._parse_date(chunk.date)
                if latest_date is None or chunk_date > latest_date:
                    latest_date = chunk_date
            except:
                continue
        
        if not latest_date:
            raise ValueError("No valid dates found in chunks")
        
        # Get all chunks from that month
        target_year = latest_date.year
        target_month = latest_date.month
        
        month_chunks = []
        for chunk in self.chunks:
            try:
                chunk_date = self._parse_date(chunk.date)
                if chunk_date.year == target_year and chunk_date.month == target_month:
                    month_chunks.append(chunk)
            except:
                continue
        
        # Sort by date, then by chunk_id to maintain order
        month_chunks.sort(key=lambda c: (self._parse_date(c.date), c.chunk_id))
        
        return month_chunks, target_year, target_month
    
    def merge_chunks_by_day(self, chunks: List[ChunkMetadata]) -> str:
        """
        Merge chunks into one document, organized by day
        Respects daily order strictly
        """
        if not chunks:
            return ""
        
        # Group chunks by date (maintain order)
        chunks_by_date = defaultdict(list)
        for chunk in chunks:
            chunks_by_date[chunk.date].append(chunk)
        
        # Get sorted dates
        sorted_dates = sorted(chunks_by_date.keys(), key=lambda d: self._parse_date(d))
        
        # Build digest
        digest_lines = []
        digest_lines.append("="*80)
        digest_lines.append("MONTHLY MARKET DATA DIGEST")
        digest_lines.append("="*80)
        digest_lines.append("")
        
        for date in sorted_dates:
            date_chunks = chunks_by_date[date]
            
            digest_lines.append("")
            digest_lines.append("─"*80)
            digest_lines.append(f"DATE: {date}")
            digest_lines.append("─"*80)
            digest_lines.append("")
            
            # Add chunks for this day in order
            for chunk in date_chunks:
                # Add source tags
                sources_tag = f"[Sources: {', '.join(chunk.sources)}]"
                digest_lines.append(sources_tag)
                digest_lines.append("")
                
                # Add chunk text
                digest_lines.append(chunk.text)
                digest_lines.append("")
                digest_lines.append("·"*80)
                digest_lines.append("")
        
        return "\n".join(digest_lines)
    
    def create_analysis_prompt(self) -> str:
        """
        Create the ultimate analysis prompt using advanced prompt engineering
        This is a PREFIX prompt - generated once, used many times
        """
        prompt = """<role>
You are an elite institutional market analyst with 20+ years of experience in macro trading, technical analysis, and multi-asset correlation modeling. Your analysis is trusted by hedge funds and institutional traders.
</role>

<task>
Analyze the provided monthly market data digest and produce a comprehensive, publication-ready strategic playbook that synthesizes all information into actionable intelligence.
</task>

<input_data>
The data digest contains 30 days of market intelligence organized chronologically, including:
- Gold (XAU/USD) price action with OHLC data
- US Dollar Index (DXY) movements
- S&P 500 (SPY) equity performance
- VIX volatility measurements
- Economic calendar events (CPI, NFP, Fed decisions, etc.)
- News headlines across multiple asset classes
- Social sentiment from trading communities
- Technical indicators (RSI, MACD, bias signals)
- Fundamental metrics (Treasury yields, credit spreads)
</input_data>

<analysis_framework>
Apply multi-layered analysis using:

1. **Temporal Analysis**: Identify key inflection points, regime changes, and momentum shifts throughout the month
2. **Cross-Asset Correlation**: Map relationships between gold, USD, equities, and volatility
3. **Causal Chain Analysis**: Connect macro events → market reactions → price movements
4. **Sentiment Divergence**: Contrast institutional positioning vs retail sentiment vs price action
5. **Technical Confluence**: Identify where technicals, fundamentals, and sentiment align or diverge
6. **Forward Implications**: Extrapolate learned patterns into forward-looking scenarios
</analysis_framework>

<output_structure>
Produce a structured report with these exact sections:

## EXECUTIVE SUMMARY
- 3-4 sentences capturing the month's dominant narrative
- Key statistic: Gold's monthly performance with % change
- Most significant event and its market impact

## MACRO ENVIRONMENT DEEP DIVE
### Economic Calendar Analysis
- Recap major data releases (CPI, NFP, PMI, etc.) with actual vs expected
- Fed policy trajectory and rate expectations evolution
- Inflation narrative shifts

### Geopolitical & Systemic Factors
- Major geopolitical events impacting risk sentiment
- Systemic risks or structural shifts observed

### Interest Rate & Credit Markets
- 10-Year Treasury yield trend and key levels
- Credit spread movements (risk appetite indicator)
- Real rates impact on gold

## GOLD (XAU/USD) COMPREHENSIVE ANALYSIS
### Price Action Summary
- Monthly: Open, High, Low, Close, % Change, ATR
- Key support levels tested and held/broken
- Key resistance levels tested and held/broken
- Intraday volatility patterns

### Technical Picture
- Trend classification (strong uptrend/downtrend/range/transition)
- RSI readings and divergences
- MACD signals and momentum shifts
- Moving average positioning (if mentioned in data)

### Fundamental Drivers
- Rank top 3 drivers by impact (e.g., USD strength, real rates, risk-off flows)
- Quantify impact where possible (e.g., "Fed pivot expectations added ~$80/oz")

### Correlation Analysis
- Gold vs USD correlation coefficient trend (inverse/positive/decorrelated)
- Gold vs SPY behavior (risk-on/risk-off dynamics)
- Gold vs VIX relationship (safe-haven flows)

## US DOLLAR (DXY) ANALYSIS
### Performance & Drivers
- Monthly DXY movement with key levels
- Primary drivers (Fed policy, yield differentials, risk sentiment)

### Impact on Gold
- Quantify USD influence on gold moves
- Identify divergences (gold up despite strong USD = significant bullish signal)

## EQUITY MARKETS (SPY) ANALYSIS
### Performance Overview
- SPY monthly return and volatility
- Sector rotation signals (if available in news data)

### Risk Appetite Assessment
- Correlation with gold (risk-on = negative, risk-off = positive)
- Notable divergences from historical patterns

## VOLATILITY (VIX) ANALYSIS
### VIX Regime
- VIX range and average for the month
- Spikes: dates, levels, catalysts
- VIX regime: complacent (<15), elevated (15-20), stressed (20-30), crisis (>30)

### Risk Sentiment Signals
- VIX-gold correlation (stress indicator)
- Complacency vs fear phases

## MARKET SENTIMENT SYNTHESIS
### Narrative Evolution
- Dominant themes at month start vs month end
- Shifts in consensus positioning
- Contrarian signals (sentiment extremes)

### News Flow Analysis
- Frequency of bullish vs bearish gold headlines
- Key news catalysts that moved markets
- Media attention cycles

### Social Sentiment (Reddit/Forums)
- Retail positioning and conviction levels
- Meme sentiment vs institutional positioning
- Sentiment divergences as contrarian indicators

## SYNTHESIS: INTERCONNECTED MARKET DYNAMICS
- Create a causal flowchart narrative: "X happened → which caused Y → resulting in Z price action"
- Identify feedback loops (e.g., USD weakness → gold strength → more USD weakness)
- Highlight regime changes (e.g., "Mid-month shift from risk-on to risk-off")

## CRITICAL INSIGHTS & TAKEAWAYS
Provide 5-7 bullet points of ACTIONABLE intelligence:
- [Insight with specific data point and implication]
- [Pattern identified and its statistical significance]
- [Divergence spotted and what it signals]
- [Key level to watch and why it matters]
- [Forward-looking setup based on current positioning]

## FORWARD-LOOKING IMPLICATIONS
### What This Month Tells Us
- Structural shifts vs noise
- Established trends likely to continue
- Potential reversal setups forming

### Key Levels & Scenarios for Next Month
- Gold: Bull scenario (above $X), Bear scenario (below $Y), Base case (range $A-$B)
- Watch list: Upcoming events that could be catalysts
- Positioning considerations based on learned correlations

</output_structure>

<writing_standards>
- **Precision**: Use exact numbers, dates, and price levels (never vague terms like "recently" or "significant")
- **Causality**: Always explain WHY something happened, not just WHAT happened
- **Objectivity**: Present bull and bear cases; acknowledge uncertainty
- **Density**: Every sentence should contain meaningful information
- **Specificity**: "Gold rallied 2.3% to $2,180 after CPI came in at 3.2% vs 3.5% expected" NOT "Gold went up on inflation data"
- **Professional Tone**: Institutional quality, no hype, no speculation without data support
</writing_standards>

<critical_thinking_requirements>
Before writing each section, ask yourself:
1. What is the PRIMARY driver vs noise?
2. Is this correlation or causation?
3. What is the market PRICING IN vs what happened?
4. Where do technicals, fundamentals, and sentiment align or conflict?
5. What would change my thesis? (falsifiability)
</critical_thinking_requirements>

<formatting_guidelines>
- Use markdown headers (##, ###) for structure
- Bold key statistics: **Gold +2.3%**, **VIX spiked to 28.4**
- Use bullet points for lists, but keep prose for causal explanations
- Include a horizontal rule (---) between major sections
- Target length: 2,000-3,000 words (comprehensive but concise)
</formatting_guidelines>

<quality_checks>
Your analysis must pass these tests:
✓ Can a trader use this to make informed decisions?
✓ Does every claim have data support from the digest?
✓ Are correlations quantified, not just stated?
✓ Is the narrative coherent (not just a data dump)?
✓ Would this be publishable in an institutional research report?
</quality_checks>

---

**Now analyze the monthly market data digest below and produce the strategic playbook following this framework exactly.**

The digest will be provided after this prompt.
"""
        return prompt
    
    def generate_monthly_digest(self) -> tuple[Path, Path]:
        """
        Main method: Generate monthly digest + prompt file
        Returns paths to both files
        """
        print("\n" + "="*80)
        print("GENERATING MONTHLY DIGEST (NO LLM CALLS)")
        print("="*80 + "\n")
        
        # Step 1: Get latest month chunks
        print("Step 1: Loading latest month chunks...")
        chunks, year, month = self.get_latest_month_chunks()
        
        print(f"✓ Found {len(chunks)} chunks for {year}-{month:02d}")
        
        # Get date range
        dates = sorted(set(chunk.date for chunk in chunks))
        print(f"  Date range: {dates[0]} to {dates[-1]}")
        print(f"  Unique days: {len(dates)}")
        
        # Step 2: Merge chunks by day
        print("\nStep 2: Merging chunks (respecting daily order)...")
        digest = self.merge_chunks_by_day(chunks)
        print(f"✓ Created digest (~{len(digest.split())} words)")
        
        # Step 3: Save digest file
        print("\nStep 3: Saving digest file...")
        digest_filename = f"monthly_digest_{year}-{month:02d}.txt"
        digest_path = self.output_folder / digest_filename
        
        with open(digest_path, 'w', encoding='utf-8') as f:
            f.write(digest)
        
        print(f"✓ Saved digest: {digest_path}")
        
        # Step 4: Generate and save prompt file
        print("\nStep 4: Generating analysis prompt (advanced prompt engineering)...")
        prompt = self.create_analysis_prompt()
        
        prompt_filename = f"analysis_prompt_{year}-{month:02d}.txt"
        prompt_path = self.output_folder / prompt_filename
        
        with open(prompt_path, 'w', encoding='utf-8') as f:
            f.write(prompt)
        
        print(f"✓ Saved prompt: {prompt_path}")
        
        # Step 5: Create combined file (prompt + digest) ready for LLM
        print("\nStep 5: Creating combined file (prompt + digest)...")
        combined_filename = f"ready_for_llm_{year}-{month:02d}.txt"
        combined_path = self.output_folder / combined_filename
        
        with open(combined_path, 'w', encoding='utf-8') as f:
            f.write(prompt)
            f.write("\n\n")
            f.write("="*80)
            f.write("\n")
            f.write("MONTHLY MARKET DATA DIGEST BEGINS HERE")
            f.write("\n")
            f.write("="*80)
            f.write("\n\n")
            f.write(digest)
        
        print(f"✓ Saved combined file: {combined_path}")
        
        # Print summary
        print("\n" + "="*80)
        print("✓ MONTHLY DIGEST GENERATION COMPLETE")
        print("="*80)
        print(f"\nGenerated 3 files for {year}-{month:02d}:")
        print(f"  1. Digest only: {digest_path.name}")
        print(f"  2. Prompt only: {prompt_path.name}")
        print(f"  3. Combined (ready for LLM): {combined_path.name}")
        print(f"\nTo analyze: Copy contents of '{combined_path.name}' to your LLM")
        print("="*80 + "\n")
        
        return digest_path, prompt_path, combined_path


def main():
    """Main execution"""
    
    try:
        generator = MonthlyDigestGenerator()
        
        # Generate digest + prompt for latest month
        digest_path, prompt_path, combined_path = generator.generate_monthly_digest()
        
        print("\n✓ Success! Files ready in monthly_summaries/ folder")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()