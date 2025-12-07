#!/usr/bin/env python3
import json
import re
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

TEXT_SUMMARIES_FOLDER = Path("daily_summaries")
VECTOR_INDEX_FOLDER = Path("vector_index")
FAISS_INDEX_PATH = VECTOR_INDEX_FOLDER / "faiss.index"
METADATA_PATH = VECTOR_INDEX_FOLDER / "metadata.jsonl"
EMBEDDING_MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
CHUNK_MIN_TOKENS = 200  # Lower minimum for better coverage
CHUNK_MAX_TOKENS = 800
BATCH_SIZE = 32


@dataclass
class ChunkMetadata:
    chunk_id: str
    date: str
    sources: List[str]
    original_file: str
    text: str
    token_count: int


class PhaseOneIndexer:
    
    def __init__(self, text_folder: Path = TEXT_SUMMARIES_FOLDER, output_folder: Path = VECTOR_INDEX_FOLDER, model_name: str = EMBEDDING_MODEL_NAME):
        self.text_folder = text_folder
        self.output_folder = output_folder
        self.model_name = model_name
        self.output_folder.mkdir(parents=True, exist_ok=True)
        print(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.embedding_dim = self.model.get_sentence_embedding_dimension()
        self.index: Optional[faiss.Index] = None
        self.metadata: List[ChunkMetadata] = []
    
    def approximate_token_count(self, text: str) -> int:
        """Approximate token count using word split"""
        return len(text.split())
    
    def detect_sources(self, text: str) -> List[str]:
        """Detect data sources from text content"""
        sources = []
        text_upper = text.upper()
        if any(kw in text_upper for kw in ["GOLD PRICE ACTION", "XAU/USD", "XAUUSD"]):
            sources.append("market")
        if any(kw in text_upper for kw in ["ECONOMIC EVENTS", "CPI", "NFP", "TREASURY", "FED"]):
            sources.append("macro")
        if any(kw in text_upper for kw in ["NEWS HIGHLIGHTS", "[NEWS]", "[MARKET]", "[GOLD]", "[VOLATILITY]"]):
            sources.append("news")
        if any(kw in text_upper for kw in ["SOCIAL SENTIMENT", "REDDIT", "r/"]):
            sources.append("reddit")
        if any(kw in text_upper for kw in ["TECHNICAL ANALYSIS", "RSI", "MACD", "BIAS"]):
            sources.append("technical")
        if any(kw in text_upper for kw in ["FUNDAMENTALS", "UNEMPLOYMENT", "INFLATION", "YIELD", "CREDIT SPREAD"]):
            sources.append("fundamentals")
        return sources if sources else ["general"]
    
    def extract_date_from_filename(self, filename: str) -> Optional[str]:
        """Extract date in YYYY-MM-DD format from filename"""
        match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
        return match.group(1) if match else None
    
    def split_by_sections(self, text: str) -> List[str]:
        """
        Split text into natural sections based on headers and content structure
        This works better than sentence-level splitting for structured summaries
        """
        # Split on major section headers (like GOLD PRICE ACTION:, ECONOMIC EVENTS:, etc.)
        section_pattern = r'(?=^[A-Z\s]+:|\n={5,}\n|MARKET SUMMARY FOR)'
        sections = re.split(section_pattern, text, flags=re.MULTILINE)
        
        # Clean up sections
        cleaned_sections = []
        for section in sections:
            section = section.strip()
            if section:
                cleaned_sections.append(section)
        
        return cleaned_sections if cleaned_sections else [text]
    
    def create_chunks_from_file(self, text: str, date: str, original_file: str, min_tokens: int = CHUNK_MIN_TOKENS, max_tokens: int = CHUNK_MAX_TOKENS) -> List[ChunkMetadata]:
        """
        Create chunks from a single file
        Strategy: Split by sections first, then by token limits if needed
        """
        # Try to split by sections first
        sections = self.split_by_sections(text)
        
        chunks = []
        chunk_counter = 0
        
        for section in sections:
            section_tokens = self.approximate_token_count(section)
            
            # If section is within limits, make it a chunk
            if min_tokens <= section_tokens <= max_tokens:
                chunks.append(ChunkMetadata(
                    chunk_id=f"{original_file.replace('.txt', '')}_chunk_{chunk_counter:04d}",
                    date=date,
                    sources=self.detect_sources(section),
                    original_file=original_file,
                    text=section,
                    token_count=section_tokens
                ))
                chunk_counter += 1
            
            # If section is too large, split it further by sentences
            elif section_tokens > max_tokens:
                sentences = re.split(r'(?<=[.!?])\s+', section)
                current_chunk = []
                current_tokens = 0
                
                for sentence in sentences:
                    sentence_tokens = self.approximate_token_count(sentence)
                    
                    if current_tokens + sentence_tokens > max_tokens and current_chunk:
                        chunk_text = ' '.join(current_chunk)
                        chunks.append(ChunkMetadata(
                            chunk_id=f"{original_file.replace('.txt', '')}_chunk_{chunk_counter:04d}",
                            date=date,
                            sources=self.detect_sources(chunk_text),
                            original_file=original_file,
                            text=chunk_text,
                            token_count=current_tokens
                        ))
                        chunk_counter += 1
                        current_chunk = []
                        current_tokens = 0
                    
                    current_chunk.append(sentence)
                    current_tokens += sentence_tokens
                
                # Add remaining sentences
                if current_chunk:
                    chunk_text = ' '.join(current_chunk)
                    if current_tokens >= min_tokens:
                        chunks.append(ChunkMetadata(
                            chunk_id=f"{original_file.replace('.txt', '')}_chunk_{chunk_counter:04d}",
                            date=date,
                            sources=self.detect_sources(chunk_text),
                            original_file=original_file,
                            text=chunk_text,
                            token_count=current_tokens
                        ))
                        chunk_counter += 1
                    elif chunks:
                        # Merge small remainder with last chunk
                        chunks[-1].text += ' ' + chunk_text
                        chunks[-1].token_count += current_tokens
            
            # If section is too small but we have no chunks yet, keep it
            elif not chunks:
                chunks.append(ChunkMetadata(
                    chunk_id=f"{original_file.replace('.txt', '')}_chunk_{chunk_counter:04d}",
                    date=date,
                    sources=self.detect_sources(section),
                    original_file=original_file,
                    text=section,
                    token_count=section_tokens
                ))
                chunk_counter += 1
            
            # If section is too small and we have chunks, merge with last chunk
            else:
                chunks[-1].text += '\n\n' + section
                chunks[-1].token_count += section_tokens
        
        return chunks
    
    def load_and_chunk_files(self) -> List[ChunkMetadata]:
        """
        Load all text files and chunk them file-by-file
        No date ordering or range expectations
        """
        if not self.text_folder.exists():
            raise FileNotFoundError(f"Text summaries folder not found: {self.text_folder}")
        
        # Get all .txt files (no sorting needed - process as-is)
        text_files = list(self.text_folder.glob("*.txt"))
        
        if not text_files:
            raise FileNotFoundError(f"No .txt files found in {self.text_folder}")
        
        print(f"\n{'='*70}")
        print(f"Found {len(text_files)} text files to process")
        print(f"{'='*70}\n")
        
        all_chunks = []
        files_processed = 0
        files_skipped = 0
        
        print("Processing files (file-by-file chunking)...")
        for text_file in tqdm(text_files, desc="Chunking files"):
            try:
                with open(text_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                if not content.strip():
                    files_skipped += 1
                    continue
                
                # Extract date (optional - use filename if no date found)
                date = self.extract_date_from_filename(text_file.name)
                if not date:
                    date = text_file.stem  # Use filename without extension
                
                # Chunk this file
                chunks = self.create_chunks_from_file(
                    text=content,
                    date=date,
                    original_file=text_file.name
                )
                
                all_chunks.extend(chunks)
                files_processed += 1
                
            except Exception as e:
                print(f"\n❌ Error processing {text_file.name}: {e}")
                files_skipped += 1
        
        print(f"\n{'='*70}")
        print(f"File Processing Summary:")
        print(f"  Files processed: {files_processed}")
        print(f"  Files skipped: {files_skipped}")
        print(f"  Total chunks created: {len(all_chunks)}")
        print(f"  Average chunks per file: {len(all_chunks)/files_processed:.1f}")
        print(f"{'='*70}\n")
        
        return all_chunks
    
    def embed_chunks(self, chunks: List[ChunkMetadata]) -> np.ndarray:
        """Generate embeddings for all chunks"""
        print("Embedding chunks...")
        texts = [chunk.text for chunk in chunks]
        embeddings = self.model.encode(
            texts,
            batch_size=BATCH_SIZE,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
        return embeddings
    
    def build_faiss_index(self, embeddings: np.ndarray) -> faiss.Index:
        """Build FAISS index from embeddings"""
        print("\nBuilding FAISS index...")
        index = faiss.IndexFlatIP(self.embedding_dim)
        index.add(embeddings.astype('float32'))
        print(f"FAISS index built with {index.ntotal} vectors")
        return index
    
    def save_index_and_metadata(self, index: faiss.Index, metadata: List[ChunkMetadata]) -> None:
        """Save FAISS index and metadata to disk"""
        print("\nSaving index and metadata...")
        
        faiss.write_index(index, str(FAISS_INDEX_PATH))
        print(f"✓ FAISS index saved to {FAISS_INDEX_PATH}")
        
        with open(METADATA_PATH, 'w', encoding='utf-8') as f:
            for chunk in metadata:
                f.write(json.dumps(asdict(chunk)) + '\n')
        print(f"✓ Metadata saved to {METADATA_PATH}")
    
    def build_index(self) -> None:
        """Main entry point to build the complete index"""
        print("\n" + "="*70)
        print("PHASE ONE INDEXER - FILE-BY-FILE EMBEDDING")
        print("="*70 + "\n")
        
        # Step 1: Load and chunk files
        chunks = self.load_and_chunk_files()
        
        if not chunks:
            print("❌ No chunks created. Exiting.")
            return
        
        # Step 2: Generate embeddings
        embeddings = self.embed_chunks(chunks)
        
        # Step 3: Build FAISS index
        self.index = self.build_faiss_index(embeddings)
        self.metadata = chunks
        
        # Step 4: Save to disk
        self.save_index_and_metadata(self.index, self.metadata)
        
        # Print summary statistics
        print("\n" + "="*70)
        print("INDEX BUILD COMPLETE")
        print("="*70)
        print(f"Total chunks: {len(chunks)}")
        print(f"Embedding dimension: {self.embedding_dim}")
        print(f"Index location: {FAISS_INDEX_PATH}")
        print(f"Metadata location: {METADATA_PATH}")
        
        # Show unique files covered
        unique_files = sorted(set(chunk.original_file for chunk in chunks))
        print(f"\nFiles indexed: {len(unique_files)}")
        
        # Show chunk distribution
        chunks_per_file = {}
        for chunk in chunks:
            chunks_per_file[chunk.original_file] = chunks_per_file.get(chunk.original_file, 0) + 1
        
        print(f"Chunk distribution:")
        print(f"  Min chunks per file: {min(chunks_per_file.values())}")
        print(f"  Max chunks per file: {max(chunks_per_file.values())}")
        print(f"  Avg chunks per file: {sum(chunks_per_file.values())/len(chunks_per_file):.1f}")
        
        print("="*70 + "\n")


class VectorRetriever:
    """Retriever for querying the built index"""
    
    def __init__(self, index_path: Path = FAISS_INDEX_PATH, metadata_path: Path = METADATA_PATH, model_name: str = EMBEDDING_MODEL_NAME):
        self.index_path = index_path
        self.metadata_path = metadata_path
        
        if not index_path.exists():
            raise FileNotFoundError(f"FAISS index not found: {index_path}")
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")
        
        print(f"Loading FAISS index from {index_path}")
        self.index = faiss.read_index(str(index_path))
        
        print(f"Loading metadata from {metadata_path}")
        self.metadata = self._load_metadata()
        
        print(f"Loading embedding model: {model_name}")
        self.model = SentenceTransformer(model_name)
        
        print(f"✓ Retriever ready: {len(self.metadata)} chunks indexed")
    
    def _load_metadata(self) -> List[ChunkMetadata]:
        """Load metadata from JSONL file"""
        metadata = []
        with open(self.metadata_path, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                metadata.append(ChunkMetadata(**data))
        return metadata
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime object (returns None if not a valid date)"""
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except:
            return None
    
    def _filter_by_date_range(self, target_date: str, days_back: int) -> List[int]:
        """Filter chunk indices by date range (lenient - skips invalid dates)"""
        target_dt = self._parse_date(target_date)
        if not target_dt:
            return []
        
        start_date = target_dt - timedelta(days=days_back)
        
        valid_indices = []
        for idx, chunk in enumerate(self.metadata):
            chunk_date = self._parse_date(chunk.date)
            if chunk_date and start_date <= chunk_date <= target_dt:
                valid_indices.append(idx)
        
        return valid_indices
    
    def retrieve(self, query: str, date: Optional[str] = None, days_back: int = 7, k: int = 10) -> List[Tuple[ChunkMetadata, float]]:
        """
        Retrieve top-k most relevant chunks for a query
        
        Args:
            query: Search query text
            date: Optional target date (YYYY-MM-DD) - if None, searches all chunks
            days_back: How many days to look back from target date (only used if date is provided)
            k: Number of results to return
            
        Returns:
            List of (ChunkMetadata, similarity_score) tuples
        """
        # Filter by date range if date provided
        if date:
            valid_indices = self._filter_by_date_range(date, days_back)
            if not valid_indices:
                print(f"⚠️  No chunks found in date range, searching all chunks instead")
                valid_indices = list(range(len(self.metadata)))
        else:
            # Search all chunks
            valid_indices = list(range(len(self.metadata)))
        
        # Encode query
        query_embedding = self.model.encode(
            [query],
            convert_to_numpy=True,
            normalize_embeddings=True
        ).astype('float32')
        
        # Get embeddings for filtered chunks
        filtered_embeddings = np.array([
            self.index.reconstruct(idx) for idx in valid_indices
        ])
        
        # Calculate similarities
        similarities = np.dot(filtered_embeddings, query_embedding.T).flatten()
        
        # Get top-k
        top_k_local = min(k, len(valid_indices))
        top_k_indices_local = np.argsort(similarities)[::-1][:top_k_local]
        
        # Build results
        results = []
        for local_idx in top_k_indices_local:
            global_idx = valid_indices[local_idx]
            chunk = self.metadata[global_idx]
            score = float(similarities[local_idx])
            results.append((chunk, score))
        
        return results


def retrieve(query: str, date: Optional[str] = None, days_back: int = 7, k: int = 10) -> List[Tuple[ChunkMetadata, float]]:
    """Convenience function for retrieval"""
    retriever = VectorRetriever()
    return retriever.retrieve(query, date, days_back, k)


def main():
    """Main execution"""
    # Build index
    indexer = PhaseOneIndexer()
    indexer.build_index()
    
    # Test retrieval
    print("\n" + "="*70)
    print("TESTING RETRIEVAL")
    print("="*70)
    
    try:
        retriever = VectorRetriever()
        
        test_query = "gold price movement"
        print(f"\nTest query: '{test_query}'")
        print(f"Searching ALL chunks (no date filter)\n")
        
        results = retriever.retrieve(test_query, date=None, k=5)
        
        if results:
            print(f"Top {len(results)} results:")
            for idx, (chunk, score) in enumerate(results, 1):
                print(f"\n[{idx}] Score: {score:.4f}")
                print(f"File: {chunk.original_file}")
                print(f"Date: {chunk.date}")
                print(f"Sources: {', '.join(chunk.sources)}")
                print(f"Text preview: {chunk.text[:150]}...")
        else:
            print("No results found")
            
    except FileNotFoundError as e:
        print(f"\n⚠️  Skipping retrieval test: {e}")
    
    print("\n" + "="*70)
    print("PHASE ONE INDEXER COMPLETE")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()