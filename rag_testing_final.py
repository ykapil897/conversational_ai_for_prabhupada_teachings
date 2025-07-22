import os
import faiss
import json
import numpy as np
import re
from datetime import datetime
import time
from sentence_transformers import SentenceTransformer
from groq import Groq
from typing import List, Dict, Any, Optional, Tuple
import streamlit as st

api_key = st.secrets["groq"]["api_key"]

# Constants for book codes and their display names
BOOK_CODES = {
    "bg": "Bhagavad Gita",
    "sb": "Srimad Bhagavatam",
    "cc": "Sri Caitanya-caritamrta",
    "unified": "Prabhupada's Complete Works",
    "letters": "Prabhupada's Letters",
}

# Book metadata paths
METADATA_PATHS = {
    "bg": "metadata/gita_metadata.json",
    "sb": "metadata/sb_metadata.json",
    "cc": "metadata/cc_metadata.json",
    "unified": "metadata/unified_metadata_split_500.json",
    "unified_grouped_books": "metadata/unified_metadata_grouped_by_book.json",
    "letters": "metadata/letters_metadata.json",
    "spokens": "metadata/spokens_metadata.json"
}

bOOK_CODES = [
    "BG", "SB", "CC", "TYS", "LCFL", "RV", "MOG", "SSR", "TQK", "EKC", "NOD",
    "POY", "TLC", "EJ", "OWK", "MM", "TLK", "PQPA", "MG", "KB", "BBD", "LOB",
    "ISO", "NOI", "TTP", "NBS", "POP"
]

def extract_book_refs(text: str):
    return [code for code in bOOK_CODES if re.search(rf'\b{code}\b|\b{code}\.\d+', text, re.IGNORECASE)]

# Book faiss index paths
INDEX_PATHS = {
    "bg": "faiss/gita_index.faiss",
    "sb": "faiss/sb_index.faiss",
    "cc": "faiss/cc_index.faiss",
    "unified": "faiss/unified_index_split_500.faiss"
}

# 1️⃣ Load embedding model once for all searches
model = SentenceTransformer("BAAI/bge-large-en")

# 2️⃣ Setup LLM client
client = Groq(api_key=api_key)

class PrabhupadaRAG:
    def __init__(self):
        # Load all metadata files upfront
        self.metadata = {}
        self.formatted_metadata = {}
        self.indices = {}
        self.book_map = {
            "tys": "Topmost Yoga System",
            "lcfl": "Life Comes From Life",
            "rv": "Raja-Vidya",
            "mog": "Message of Godhead",
            "ssr": "Science of Self Realization",
            "tqk": "Teachings of Queen Kunti",
            "ekc": "Elevation to Krsna Consciousness",
            "nod": "Nectar of Devotion",
            "poy": "Perfection of Yoga",
            "tlc": "Teachings of Lord Caitanya",
            "ej": "Easy Journey to Other Planets",
            "owk": "On the Way to Krsna",
            "mm": "Mukunda-mala-stotra",
            "tlk": "Teachings of Lord Kapila",
            "pqpa": "Perfect Questions Perfect Answers",
            "mg": "Matchless Gift",
            "kb": "Krsna Book",
            "bbd": "Beyond Birth and Death",
            "lob": "Light of the Bhagavata",
            "iso": "Sri Isopanisad",
            "noi": "Nectar of Instruction",
            "ttp": "Transcendental Teachings of Prahlada Maharaja",
            "nbs": "Narada Bhakti Sutra",
            "pop": "Path of Perfection"
        } 

        # Initialize book data
        for book_code in BOOK_CODES:
            self._load_book_data(book_code)
        
        # Cache for storing retrieved but unused results
        self.cached_results = {}
    
    def _load_book_data(self, book_code: str):
        """Load metadata and index for a specific book"""
        try:
            # Load metadata
            with open("metadata/unified_metadata_grouped_by_book.json", "r") as f:
                self.metadata["unified_grouped_books"] = json.load(f)

            with open(METADATA_PATHS[book_code], "r") as f:
                self.metadata[book_code] = json.load(f)
            
            # Create formatted metadata (only if not already pre-formatted in the JSON)
            self.formatted_metadata[book_code] = []
            for entry in self.metadata[book_code]:
                # Skip if already formatted
                if isinstance(entry, str) and "REFERENCE:" in entry:
                    self.formatted_metadata[book_code].append(entry)
                    continue
                
                # Extract information from entry
                verse_id = self._extract_verse_id(entry["url"], book_code)
                verse_text = entry.get("verse_text", "")
                translation = entry.get("translation", "")
                purport = entry.get("purport", "")
                url = entry.get("url", "")
                book_title = BOOK_CODES[book_code]
                
                # Format entry
                verse_part = f"VERSE: {verse_text}" if verse_text else ""
                translation_part = f"TRANSLATION: {translation}" if translation else ""
                purport_part = f"PURPORT BY SRILA PRABHUPADA: {purport}" if purport else ""
                url_part = f"URL: {url}" if url else ""
                
                formatted_text = f"REFERENCE: {verse_id} ({book_title})\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\n{url_part}"
                self.formatted_metadata[book_code].append(formatted_text)
            
            # Load FAISS index
            self.indices[book_code] = faiss.read_index(INDEX_PATHS[book_code])
            
            print(f"✅ Loaded {book_code} data: {len(self.metadata[book_code])} entries")
            
        except Exception as e:
            print(f"⚠️ Error loading {book_code} data: {str(e)}")
            # Initialize empty to avoid errors
            self.metadata[book_code] = []
            self.formatted_metadata[book_code] = []
            self.indices[book_code] = None
    
    def _extract_verse_id(self, url: str, book_code: str) -> str:
        """Extract verse ID from URL based on book type"""
        if book_code == "bg":
            # Bhagavad Gita URLs: https://github.com/iskconpress/books/tree/master/bg/1/1.txt
            match = re.search(r'bg/(\d+)/(\d+)/?', url)
            if match:
                return f"Bg. {match.group(1)}.{match.group(2)}"
        elif book_code == "sb":
            # Srimad Bhagavatam URLs: https://github.com/iskconpress/books/tree/master/sb/1/1/1.txt
            match = re.search(r'sb/(\d+)/(\d+)/(\d+)', url)
            if match:
                return f"SB {match.group(1)}.{match.group(2)}.{match.group(3)}"
        elif book_code == "cc":
            # Caitanya Caritamrta URLs: https://github.com/iskconpress/books/tree/master/cc/adi/1/1.txt
            match = re.search(r'cc/(\w+)/(\d+)/(\d+)', url)
            if match:
                lila = match.group(1)
                chapter = match.group(2)
                verse = match.group(3)
                # Map 'adi', 'madhya', 'antya' to their abbreviations
                lila_map = {"adi": "Adi", "madhya": "Madhya", "antya": "Antya"}
                lila_abbrev = lila_map.get(lila.lower(), lila)
                return f"CC {lila_abbrev} {chapter}.{verse}"
        elif book_code == "unified":
            # Unified URLs: https://github.com/iskconpress/books/blob/master/tys/1.txt/
            match = re.search(r'master/(\w+)/(\w+)\.txt', url)
            if match:
                book_abbr = match.group(1)
                chapter = match.group(2)
                # Map abbreviations to full names when possible

                book_name = self.book_map.get(book_abbr, book_abbr.upper())
                return f"{book_name} {chapter}"
        
        # Default fallback: use URL as the ID
        return url.split("/")[-1].replace(".txt", "")

    def get_context_for_quiz(self, original_query: str) -> Dict[str, Any]:
        """
        Process a query to get context for quiz generation
        
        Args:
            original_query: The user's original question
            
        Returns:
            Dictionary with query processing results up to context retrieval
        """

        # Load history for context-aware query refinement
        history_path = "history.json"
        try:
            with open(history_path, "r") as f:
                history = json.load(f)
                previous_queries = history[-3:]
        except (FileNotFoundError, json.JSONDecodeError):
            previous_queries = []

        # Format previous queries for prompt
        previous_context_block = "\n".join(
            [f"{i+1}. {q}" for i, q in enumerate(previous_queries)]
        ) if previous_queries else "None"
        
        print(f"🔍 Quiz request: '{original_query}'")
        
        # STAGE 1: Query Refinement with LLM
        query_refinement_response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a query refinement expert for Srila Prabhupada's literary works. "
                        "Your job is to convert user quiz requests into effective search queries that will match relevant content. "
                        "Since this is for quiz generation, focus on finding content-rich passages that would be suitable for creating quiz questions. "
                        "Maintain the original intent and key concepts from the user's query. "
                        "Make the refined query concise and focused on key concepts that would appear in the text.\n\n"
                        
                        "IMPORTANT - BOOK REFERENCE STANDARDIZATION:\n"
                        "Always standardize book references to these exact formats:\n"
                        "- Bhagavad Gita → 'Bg.' (e.g., 'Bg. 2.45')\n"
                        "- Srimad Bhagavatam → 'SB' (e.g., 'SB 1.1.1')\n"
                        "- Chaitanya Charitamrita → 'Cc.' with proper division (e.g., 'Cc. Adi 1.10', 'Cc. Madhya 5.1', 'Cc. Antya 4.1')\n"
                        "- Teaching of Queen Kunti → 'TQK'\n"
                        "- Nectar of Devotion → 'NOD'\n"
                        "- Life Comes From Life → 'LCFL'\n"
                        "- Science of Self-Realization → 'SSR'\n"
                        "- Topmost Yoga System → 'TYS'\n"
                        "- Raja-Vidya → 'RV'\n"
                        "- Message of Godhead → 'MOG'\n"
                        "- Elevation to Krsna Consciousness → 'EKC'\n"
                        "- Perfection of Yoga → 'POY'\n"
                        "- Teachings of Lord Caitanya → 'TLC'\n"
                        "- Easy Journey to Other Planets → 'EJ'\n"
                        "- On the Way to Krsna → 'OWK'\n"
                        "- Mukunda-mala-stotra → 'MM'\n"
                        "- Teachings of Lord Kapila → 'TLK'\n"
                        "- Perfect Questions Perfect Answers → 'PQPA'\n"
                        "- Matchless Gift → 'MG'\n"
                        "- Krsna Book → 'KB'\n"
                        "- Beyond Birth and Death → 'BBD'\n"
                        "- Light of the Bhagavata → 'LOB'\n"
                        "- Sri Isopanisad → 'ISO'\n"
                        "- Nectar of Instruction → 'NOI'\n"
                        "- Transcendental Teachings of Prahlada Maharaja → 'TTP'\n"
                        "- Narada Bhakti Sutra → 'NBS'\n"
                        "- Path of Perfection → 'POP'\n\n"
                        
                        "IMPORTANT - BOOK REFERENCE STANDARDIZATION:\n"
                        "Always standardize book references to these exact formats:\n"
                        "- Bhagavad Gita → 'Bg.' (e.g., 'Bg. 2.45')\n"
                        "- Srimad Bhagavatam → 'SB' (e.g., 'SB 1.1.1')\n"
                        "- Chaitanya Charitamrita → 'Cc.' with proper division (e.g., 'Cc. Adi 1.10', 'Cc. Madhya 5.1', 'Cc. Antya 4.1')\n"
                        "... (other books unchanged) ...\n\n"

                        "🆕 ADDITIONAL RULES:\n"
                        "- If the user mentions keywords like 'chapter 5 of Bhagavad Gita', 'canto 4 chapter 3 in Srimad Bhagavatam', or 'Madhya 2 verse 12 in Chaitanya Charitamrita', then standardize these into verse formats such as 'Bg. 5', 'SB 4.3', or 'Cc. Madhya 2.12' respectively.\n"
                        "- For Bhagavad Gita: always convert 'chapter X verse Y' into 'Bg. X.Y'\n"
                        "- For Srimad Bhagavatam: convert 'canto X chapter Y verse Z' into 'SB X.Y.Z'\n"
                        "- For Chaitanya Charitamrita: convert 'Adi/Madhya/Antya chapter Y verse Z' into 'Cc. Adi Y.Z', etc.\n"
                        "- If the user gives ambiguous references like 'chapter 10' without book name, do NOT assume any book—ask for clarification or keep it broad.\n"
                        "- Avoid expanding or rewriting already correct references (e.g., don’t change 'Bg. 2.13' into something else).\n"

                        "Be precise and focus on finding content suitable for quiz generation."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Here's a request for quiz content:\n{original_query}\n\n"
                        "Return only the refined search query that will help find relevant content for creating quiz questions:"
                    )
                },
            ],
            model="llama3-70b-8192",
            temperature=0.2,
            top_p=0.2,
            max_tokens=100
        )

        refined_query = query_refinement_response.choices[0].message.content.strip()
        refined_query = re.sub(r'^"(.+)"$', r'\1', refined_query)
        print(f"🔄 Refined query for quiz content: '{refined_query}'")

        # STAGE 2: Parse query for specific book/chapter/verse requests
        book_mentions = {
            "bg": ["bhagavad gita", "gita", "bg", "bhagavad-gita", "bg."],
            "sb": ["srimad bhagavatam", "bhagavatam", "sb", "srimad-bhagavatam", "sb."],
            "cc": ["caitanya caritamrta", "chaitanya charitamrita", "cc", "caitanya-caritamrta", "cc."],
        }
        
        search_books = []
        
        # Check for specific book mentions in the query
        for book_code, patterns in book_mentions.items():
            if any(pattern in refined_query.lower() for pattern in patterns):
                search_books.append(book_code)
                print(f"📚 Detected specific book for quiz: {BOOK_CODES[book_code]}")
        
        # Check for unified collection book mentions
        unified_book_codes = self._detect_unified_books(refined_query)

        if not search_books:
            search_books = ["unified"]  # Default to unified collection if no specific books found

        # Extract book-specific reference patterns
        retrieved_context = ""
        retrieval_sources = []
        
       # BG verse pattern: Check for "Bg. X.Y" patterns
        bg_verse_match = re.search(r'(?:Bg\.)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        print(f"BG verse match: {bg_verse_match}")

        # SB verse pattern: Check for "SB X.Y.Z" patterns
        sb_verse_match = re.search(r'(?:SB)[.\s]*(\d+)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        print(f"SB verse match: {sb_verse_match}")

        # CC verse pattern: Check for "Cc. [Lila] X.Y" patterns
        cc_verse_match = re.search(r'(?:Cc\.)[.\s]*(Adi|Madhya|Antya)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        print(f"CC verse match: {cc_verse_match}")

        # General chapter pattern: "Chapter X" patterns
        # chapter_match = re.search(r'(?:chapter|ch)[:\-\s.,]?(\d+)', refined_query, re.IGNORECASE)
        chapter_match = re.search(
            # r'(?:(?:chapter|ch)[\s:.\-]*(\d+)(?:\s*(?:of)?\s*(Bhagavad[\s\-]?Gita|Gita|BG))?)|'  # chapter 5 of Bhagavad Gita
            # r'(?:canto\s*(\d+)\s*(?:chapter|ch)[\s:.\-]*(\d+))|'                                 # canto 4 chapter 3
            r'(?:Cc\.\s*(Adi|Madhya|Antya)\s*(\d+))|'                                     # Cc. Madhya 2.12
            r'(?:SB\s*(\d+)\.(\d+))|'                                                            # SB 4.3
            r'(?:Bg\.\s*(\d+))',                                                                 # Bg. 5
            refined_query,
            re.IGNORECASE
        )

        print(f"Chapter match: {chapter_match}")


        search_start_time = time.time()

        # STAGE 3: Retrieve content based on book and specificity
        if bg_verse_match:
            # Handle Bhagavad Gita specific verse
            print(f"📖 Bhagavad Gita verse requested for quiz: {bg_verse_match}")
            specific_reference = self._process_bg_verse_reference(bg_verse_match)
            if specific_reference:
                retrieved_context, retrieval_sources = specific_reference
        
        elif sb_verse_match:
            # Handle Srimad Bhagavatam specific verse
            print(f"📖 Srimad Bhagavatam verse requested for quiz: {sb_verse_match}")
            specific_reference = self._process_sb_verse_reference(sb_verse_match)
            if specific_reference:
                retrieved_context, retrieval_sources = specific_reference
        
        elif cc_verse_match:
            # Handle Caitanya Caritamrta specific verse
            print(f"📖 Caitanya Caritamrta verse requested for quiz: {cc_verse_match}")
            specific_reference = self._process_cc_verse_reference(cc_verse_match)
            if specific_reference:
                retrieved_context, retrieval_sources = specific_reference
        
        # Process unified specific book reference
        elif unified_book_codes:
            print(f"📖 Unified book reference requested for quiz: {unified_book_codes}")
            multi_book_result = self._process_multiple_unified_books(unified_book_codes, refined_query)
            if multi_book_result:
                retrieved_context, retrieval_sources, cached_results = multi_book_result
        
        elif chapter_match:
            print(f"📖 Chapter requested for quiz: {chapter_match.group(1)}")
            # Handle chapter request across specified books
            chapter = chapter_match.group(1)

            # Try each book for the chapter based on detected books
            found_chapter = False
            for book in search_books:
                specific_reference = self._process_chapter_reference(chapter, book, refined_query)
                if specific_reference and specific_reference[0]:  # Check if content was found
                    retrieved_context, retrieval_sources = specific_reference
                    found_chapter = True
                    break
                    
            # If chapter not found in detected books but detected books weren't "unified",
            # try unified as fallback
            if not found_chapter and "unified" not in search_books:
                specific_reference = self._process_chapter_reference(chapter, "unified", refined_query)
                if specific_reference and specific_reference[0]:
                    retrieved_context, retrieval_sources = specific_reference
        
        # If no specific reference matched or was found, do semantic search
        if not retrieved_context:
            # Do semantic search across detected books
            print("🔍 Performing semantic search for quiz content...")
            retrieved_context, retrieval_sources, _ = self._semantic_search(
                refined_query, search_books
            )
        
        search_elapsed_time = time.time() - search_start_time
        print(f"⏱️ Quiz content retrieval time: {search_elapsed_time:.2f} seconds")

        # For quiz generation, we want to return more comprehensive content
        # so we'll get more results if the initial results are too small
        if len(retrieval_sources) < 2:
            # Try to get more content by broadening the search
            print("⚠️ Limited quiz content found, broadening search...")
            broader_query = re.sub(r'(specific|exact|precise|particular)\s+', '', refined_query)
            retrieved_context_additional, retrieval_sources_additional, _ = self._semantic_search(
                broader_query, search_books
            )
            
            if retrieved_context_additional:
                retrieved_context += "\n\n===== ADDITIONAL CONTENT =====\n\n" + retrieved_context_additional
                retrieval_sources.extend(retrieval_sources_additional)

        return {
            "original_query": original_query,
            "refined_query": refined_query,
            "retrieved_context": retrieved_context,
            "retrieval_sources": retrieval_sources,
            "search_time": search_elapsed_time,
    }

    def process_query(
        self,
        original_query: str,
        # preferred_books: List[str] = None,
        prabhupada_ratio: int = 70,
        answer_length: str = "medium",
        answer_format: str = "conversational",
        devotee_level: str = "intermediate"
    ) -> Dict[str, Any]:
        """
        Process a query with customizable preferences:
        
        Args:
            original_query: The user's original question
            preferred_books: List of book codes to search in (bg, sb, cc, unified)
            prabhupada_ratio: Percentage of content that should be Prabhupada's direct words (0-100)
            answer_length: Desired response length (short, medium, long)
            answer_format: Format style (conversational, academic, scriptural)
            devotee_level: Level of spiritual understanding (neophyte, intermediate, advanced)
            
        Returns:
            Dictionary with query processing results
        """
        overall_start_time = time.time()

        self.last_prabhupada_ratio = prabhupada_ratio
        self.last_answer_length = answer_length
        self.last_answer_format = answer_format
        self.last_devotee_level = devotee_level   
        
        # def validate_book_refs(prev_refined, user_query):
        #     current_refs = extract_book_refs(user_query)
        #     refined_refs = extract_book_refs(prev_refined)

        #     # If the current query specifies a book, but the refined one doesn't honor it
        #     if current_refs and not any(ref in refined_refs for ref in current_refs):
        #         return user_query.strip()
        #     return prev_refined

        # Store query ID for caching
        query_id = hash(original_query) #+ str(preferred_books))
        
        history_path = "history.json"
        try:
            with open(history_path, "r") as f:
                history = json.load(f)
                previous_queries = history[-3:]
        except (FileNotFoundError, json.JSONDecodeError):
            previous_queries = []

        # Save current query to history
        history = previous_queries + [original_query]
        with open(history_path, "w") as f:
            json.dump(history, f, indent=2)
        
        previous_context_block = "\n".join(
            [f"{i+1}. {q}" for i, q in enumerate(previous_queries)]
        ) if previous_queries else "None"
        
        print(f"🔍 Original query: '{original_query}'")
        # print(f"📚 Searching in: {', '.join([BOOK_CODES[b] for b in preferred_books])}")
        
        # 1️⃣ STAGE 1: Query Refinement with LLM
        # book_list = ", ".join([BOOK_CODES[b] for b in preferred_books])
        # source_instruction = f"Focus on these sources: {book_list}. " if preferred_books else ""
        
        query_refinement_response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a query refinement expert for Srila Prabhupada's literary works. "
                        "Your job is to convert user queries into effective search queries that will match relevant content. "
                        "Maintain the original intent and key concepts from the user's query. "
                        "Make the refined query concise and focused on key concepts that would appear in the text.\n\n"
                        """ 📘 BOOK REFERENCE RULES (Strictly Follow):

                        1. If the current user query includes a book reference (e.g., Bg., SB, Cc., SSR, PoP, etc.):
                        → IGNORE any book references from previous questions or answers.

                        2. You may carry over previous context only if:
                        a. The current query refers to it explicitly (e.g., 'as discussed earlier', 'as mentioned').
                        b. The query uses pronouns needing context (e.g., 'this teaching', 'that example').
                        c. There are NO book references in the current query.

                        3. If a new book is mentioned, prioritize it above all else — even if it conflicts with previous context.

                        EXAMPLES:

                        ✅ Example 1:
                        Prev Q: "Explain karma yoga in Bg. 3"
                        Curr Q: "What about SB 1.1.1?"
                        → Refined: "SB 1.1.1" (ignore Bg.)

                        ✅ Example 2:
                        Prev Q: "What are the three modes in Bg. 14?"
                        Curr Q: "How does this apply to leadership?"
                        → Refined: "How do the three modes from Bg. 14 apply to leadership?"

                        DO NOT hallucinate or merge books unless user indicates they are related."""

                        "IMPORTANT - BOOK REFERENCE STANDARDIZATION:\n"
                        "Always standardize book references to these exact formats:\n"
                        "- Bhagavad Gita → 'Bg.' (e.g., 'Bg. 2.45')\n"
                        "- Srimad Bhagavatam → 'SB' (e.g., 'SB 1.1.1')\n"
                        "- Chaitanya Charitamrita → 'Cc.' with proper division (e.g., 'Cc. Adi 1.10', 'Cc. Madhya 5.1', 'Cc. Antya 4.1')\n"
                        "- Teaching of Queen Kunti → 'TQK'\n"
                        "- Nectar of Devotion → 'NOD'\n"
                        "- Life Comes From Life → 'LCFL'\n"
                        "- Science of Self-Realization → 'SSR'\n"
                        "- Topmost Yoga System → 'TYS'\n"
                        "- Raja-Vidya → 'RV'\n"
                        "- Message of Godhead → 'MOG'\n"
                        "- Elevation to Krsna Consciousness → 'EKC'\n"
                        "- Perfection of Yoga → 'POY'\n"
                        "- Teachings of Lord Caitanya → 'TLC'\n"
                        "- Easy Journey to Other Planets → 'EJ'\n"
                        "- On the Way to Krsna → 'OWK'\n"
                        "- Mukunda-mala-stotra → 'MM'\n"
                        "- Teachings of Lord Kapila → 'TLK'\n"
                        "- Perfect Questions Perfect Answers → 'PQPA'\n"
                        "- Matchless Gift → 'MG'\n"
                        "- Krsna Book → 'KB'\n"
                        "- Beyond Birth and Death → 'BBD'\n"
                        "- Light of the Bhagavata → 'LOB'\n"
                        "- Sri Isopanisad → 'ISO'\n"
                        "- Nectar of Instruction → 'NOI'\n"
                        "- Transcendental Teachings of Prahlada Maharaja → 'TTP'\n"
                        "- Narada Bhakti Sutra → 'NBS'\n"
                        "- Path of Perfection → 'POP'\n\n"
                        
                        "WHEN TO USE PREVIOUS CONTEXT - ONLY in these specific cases:\n"
                        "1. The current query EXPLICITLY refers to previous content (using terms like 'previously discussed', 'as mentioned', etc.)\n"
                        "2. The current query is clearly a FOLLOW-UP question that depends on previous context\n"
                        "3. The current query contains PRONOUNS or REFERENCES (like 'this', 'it', 'these teachings') that need context\n\n"
                        "4. If the previous query and current query is entirely different or if the reference mentioned is different from the previous query, then don't include previous query into current."
                        "If none of these conditions are met, keep the query standalone without adding previous context.\n\n"
                        

                        "IMPORTANT - BOOK REFERENCE STANDARDIZATION:\n"
                        "Always standardize book references to these exact formats:\n"
                        "- Bhagavad Gita → 'Bg.' (e.g., 'Bg. 2.45')\n"
                        "- Srimad Bhagavatam → 'SB' (e.g., 'SB 1.1.1')\n"
                        "- Chaitanya Charitamrita → 'Cc.' with proper division (e.g., 'Cc. Adi 1.10', 'Cc. Madhya 5.1', 'Cc. Antya 4.1')\n"
                        "... (other books unchanged) ...\n\n"

                        "Example: If a previous query was about 'karma yoga in Bg. 3', refine 'tell me more about this' to 'more about karma yoga in Bg. 3'\n\n"

                        "🆕 ADDITIONAL RULES:\n"
                        "- If the user mentions keywords like 'chapter 5 of Bhagavad Gita', 'canto 4 chapter 3 in Srimad Bhagavatam', or 'Madhya 2 verse 12 in Chaitanya Charitamrita', then standardize these into verse formats such as 'Bg. 5', 'SB 4.3', or 'Cc. Madhya 2.12' respectively.\n"
                        "- For Bhagavad Gita: always convert 'chapter X verse Y' into 'Bg. X.Y'\n"
                        "- For Srimad Bhagavatam: convert 'canto X chapter Y verse Z' into 'SB X.Y.Z'\n"
                        "- For Chaitanya Charitamrita: convert 'Adi/Madhya/Antya chapter Y verse Z' into 'Cc. Adi Y.Z', etc.\n"
                        "- If the user gives ambiguous references like 'chapter 10' without book name, do NOT assume any book—ask for clarification or keep it broad.\n"
                        "- Avoid expanding or rewriting already correct references (e.g., don’t change 'Bg. 2.13' into something else).\n"

                        "Be precise, concise, and ensure all references follow the standard format pattern for optimal searchability."
                    )
                },
                {
                    "role": "user",
                    "content": (
                        f"Here are ALL the user's previous queries:\n{previous_context_block}\n\n"
                        f"Current query:\n{original_query}\n\n"
                        
                        "Return only the refined version of the current query that explicitly includes relevant topics from previous queries when references to 'previously discussed' content are made:"
                    )
                },
            ],
            model="llama3-70b-8192",
            temperature=0.2,
            top_p=0.2,
            max_tokens=100
        )

        refined_query = query_refinement_response.choices[0].message.content.strip()
        # refined_query = validate_book_refs(refined_query, original_query)
        refined_query = re.sub(r'^"(.+)"$', r'\1', refined_query)
        print(f"🔄 Refined query: '{refined_query}'")

        # 2️⃣ STAGE 2: Parse query for specific book/chapter/verse requests
        book_mentions = {
            "bg": ["bhagavad gita", "gita", "bg", "bhagavad-gita", "bg."],
            "sb": ["srimad bhagavatam", "bhagavatam", "sb", "srimad-bhagavatam", "sb."],
            "cc": ["caitanya caritamrta", "chaitanya charitamrita", "cc", "caitanya-caritamrta", "cc."],
        }
        
        search_books = []

        # Check for specific book mentions in the query
        for book_code, patterns in book_mentions.items():
            if any(pattern in refined_query.lower() for pattern in patterns):
                search_books.append(book_code)
                print(f"📚 Detected specific book: {BOOK_CODES[book_code]}")
        
        # Check for unified collection book mentions
        unified_book_codes = self._detect_unified_books(refined_query)

        if not search_books:
            search_books = ["unified"]  # Default to unified collection if no specific books found

        # Extract book-specific reference patterns
        retrieved_context = ""
        retrieval_sources = []
        cached_results = []
        
        # BG verse pattern: Check for "Bg. X.Y" patterns
        bg_verse_match = re.search(r'(?:Bg\.)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        print(f"BG verse match: {bg_verse_match}")

        # SB verse pattern: Check for "SB X.Y.Z" patterns
        sb_verse_match = re.search(r'(?:SB)[.\s]*(\d+)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        print(f"SB verse match: {sb_verse_match}")

        # CC verse pattern: Check for "Cc. [Lila] X.Y" patterns
        cc_verse_match = re.search(r'(?:Cc\.)[.\s]*(Adi|Madhya|Antya)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        print(f"CC verse match: {cc_verse_match}")

        # General chapter pattern: "Chapter X" patterns
        # chapter_match = re.search(r'(?:chapter|ch)[:\-\s.,]?(\d+)', refined_query, re.IGNORECASE)
        chapter_match = re.search(
            # r'(?:(?:chapter|ch)[\s:.\-]*(\d+)(?:\s*(?:of)?\s*(Bhagavad[\s\-]?Gita|Gita|BG))?)|'  # chapter 5 of Bhagavad Gita
            # r'(?:canto\s*(\d+)\s*(?:chapter|ch)[\s:.\-]*(\d+))|'                                 # canto 4 chapter 3
            r'(?:Cc\.\s*(Adi|Madhya|Antya)\s*(\d+)\.(\d+))|'                                     # Cc. Madhya 2.12
            r'(?:SB\s*(\d+)\.(\d+))|'                                                            # SB 4.3
            r'(?:Bg\.\s*(\d+))',                                                                 # Bg. 5
            refined_query,
            re.IGNORECASE
        )

        print(f"Chapter match: {chapter_match}")

        search_start_time = time.time()

        # 3️⃣ STAGE 3: Retrieve content based on book and specificity
        if bg_verse_match: 
            # Handle Bhagavad Gita specific verse
            print(f"📖 Bhagavad Gita verse requested: {bg_verse_match}")
            specific_reference =  self._process_bg_verse_reference(bg_verse_match)
            if specific_reference:
                retrieved_context, retrieval_sources = specific_reference
        
        elif sb_verse_match: 
            # Handle Srimad Bhagavatam specific verse
            print(f"📖 Srimad Bhagavatam verse requested: {sb_verse_match}")
            specific_reference = self._process_sb_verse_reference(sb_verse_match)
            if specific_reference:
                retrieved_context, retrieval_sources = specific_reference
        
        elif cc_verse_match: 
            # Handle Caitanya Caritamrta specific verse
            print(f"📖 Caitanya Caritamrta verse requested: {cc_verse_match}")
            specific_reference = self._process_cc_verse_reference(cc_verse_match)
            if specific_reference:
                retrieved_context, retrieval_sources = specific_reference
        
        # Process unified specific book reference
        elif unified_book_codes:
            print(f"📖 Unified book reference requested: {unified_book_codes}")
            multi_book_result = self._process_multiple_unified_books(unified_book_codes, refined_query)
            if multi_book_result:
                retrieved_context, retrieval_sources, cached_results = multi_book_result

                # Store cached results for later use
                query_id = hash(original_query)
                self.cached_results[query_id] = cached_results
    
        
        elif chapter_match:
            print(f"📖 Chapter requested: {chapter_match.group(1)}")
            # Handle chapter request across specified books
            chapter = chapter_match.group(1)
            print(f"📖 Chapter requested: {chapter}")

            # Try each book for the chapter based on detected books
            found_chapter = False
            for book in search_books:
                specific_reference = self._process_chapter_reference(chapter, book, refined_query)
                if specific_reference and specific_reference[0]:  # Check if content was found
                    retrieved_context, retrieval_sources = specific_reference
                    found_chapter = True
                    break
                    
            # If chapter not found in detected books but detected books weren't "unified",
            # try unified as fallback
            if not found_chapter and "unified" not in search_books:
                specific_reference = self._process_chapter_reference(chapter, "unified", refined_query)
                if specific_reference and specific_reference[0]:
                    retrieved_context, retrieval_sources = specific_reference
        
        print(f"Done upto here ")

        # If no specific reference matched or was found, do semantic search
        if not retrieved_context:
            # Do semantic search across preferred books
            print("🔍 Performing semantic search across preferred books...")
            retrieved_context, retrieval_sources, cached_results = self._semantic_search(
                refined_query, search_books # preferred_books
            )
            
            # Store cached results for later use
            self.cached_results[query_id] = cached_results
        
        search_elapsed_time = time.time() - search_start_time
        print(f"⏱️ Total search time: {search_elapsed_time:.2f} seconds")

        stage_4_time = time.time()

        # 4️⃣ STAGE 4: Extract Key Facts and Citations 
        facts_extraction = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": self._get_facts_extraction_prompt(devotee_level)
                },
                {
                    "role": "user", 
                    "content": f"Context from Srila Prabhupada's works:\n\n{retrieved_context}\n\nUser question: {original_query}\n\nExtract key facts with proper attribution and URLs:"
                },
            ],
            model="llama3-70b-8192",
            temperature=0.2,
            top_p=0.2,
            max_tokens=self._get_token_length(answer_length, stage="facts")
        )

        extracted_facts = facts_extraction.choices[0].message.content
        
        overall_stage_4_time = time.time() - stage_4_time
        print(f"Stage 4 overall time: {overall_stage_4_time}")

        # 5️⃣ STAGE 5: Transform facts based on user preferences
        # If prabhupada_ratio is high, keep just facts without transformation

        stage_5_time = time.time()

        if prabhupada_ratio >= 90:
            # High Prabhupada ratio - minimal AI enhancement
            final_answer = extracted_facts
            print("\n📝 Final Answer (Original Texts Emphasis):")
        else:
            # Transform into user-friendly explanation
            user_friendly_response = client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": self._get_user_friendly_prompt(answer_format, devotee_level, prabhupada_ratio)
                    },
                    {
                        "role": "user", 
                        "content": (
                            f"Here are the extracted facts with attributions and URLs:\n\n{extracted_facts}\n\n"
                            f"User question: {original_query}\n\n"
                            f"Prabhupada content ratio: {prabhupada_ratio}% (higher means more direct quotes)\n"
                            f"Preferred length: {answer_length}\n"
                            f"Format: {answer_format}\n"
                            f"Devotee level: {devotee_level}\n\n"
                            "Transform these facts into an appropriate explanation that includes direct quotes from Srila Prabhupada:"
                        )
                    },
                ],
                model="llama3-70b-8192",
                temperature=0.4,
                top_p=0.5,
                max_tokens=self._get_token_length(answer_length, stage="final")
            )
            
            final_answer = user_friendly_response.choices[0].message.content
            print("\n🧘 Final User-Friendly Answer:")
        
        print(final_answer)

        overall_stage_5_time = time.time() - stage_5_time
        print(f"Stage 5 Overall Time: {overall_stage_5_time}")

        # Calculate actual ratio of Prabhupada quotes in the answer
        actual_ratio = self._calculate_prabhupada_ratio(final_answer, extracted_facts)
        
        overall_elapsed_time = time.time() - overall_start_time
        print(f"⏱️ Total query processing time: {overall_elapsed_time:.2f} seconds")
        
        time_ = {
            "overall_stage_4_time": overall_stage_4_time, 
            "overall_stage_5_time": overall_stage_5_time, 
            "overall_elapsed_time": overall_elapsed_time, 
            "search_elapsed_time": search_elapsed_time
        }
        
        # Save the results for analysis
        results_path = self._save_query_results(
            original_query, 
            refined_query, 
            retrieved_context, 
            extracted_facts, 
            final_answer, 
            retrieval_sources,
            actual_ratio, 
            time_
        )

        return {
            "original_query": original_query,
            "refined_query": refined_query,
            "retrieved_context": retrieved_context,
            "extracted_facts": extracted_facts,
            "final_answer": final_answer,
            "retrieval_sources": retrieval_sources,
            "prabhupada_ratio": actual_ratio,
            "cache_id": query_id,
            "search_books": search_books, 
            "analysis_path": results_path,
            "search_time": search_elapsed_time,
            "total_time": overall_elapsed_time
        }
    
    def _detect_unified_books(self, query: str) -> Optional[str]:
        """Detect if the query is specifically asking for a book in the unified collection"""

        start_time = time.time()
        # Map of book abbreviations and possible mentions
        unified_book_patterns = {
            "tys": [r'topmost\s*yoga', r'\btys\b'],
            "lcfl": [r'life\s*comes?\s*from\s*life', r'\blcfl\b'],
            "rv": [r'raja\s*vidya', r'\brv\b'],
            "mog": [r'message\s*of\s*godhead', r'\bmog\b'], 
            "ssr": [r'science\s*of\s*self', r'\bssr\b'],
            "tqk": [r'teachings?\s*of\s*queen\s*kunti', r'\btqk\b'],
            "ekc": [r'elevation\s*to\s*k[rṛ][sṣ]na', r'\bekc\b'],
            "nod": [r'nectar\s*of\s*devotion', r'\bnod\b'],
            "poy": [r'perfection\s*of\s*yoga', r'\bpoy\b'],
            "tlc": [r'teachings?\s*of\s*lord\s*caitanya', r'\btlc\b'],
            "ej": [r'easy\s*journey', r'\bej\b'],
            "owk": [r'on\s*the\s*way\s*to\s*k[rṛ][sṣ]na', r'\bowk\b'],
            "mm": [r'mukunda\s*mala', r'\bmm\b'],
            "tlk": [r'teachings?\s*of\s*lord\s*kapila', r'\btlk\b'],
            "pqpa": [r'perfect\s*questions', r'\bpqpa\b'],
            "mg": [r'matchless\s*gift', r'\bmg\b'],
            "kb": [r'k[rṛ][sṣ]na\s*book', r'\bkb\b'],
            "bbd": [r'beyond\s*birth\s*and\s*death', r'\bbbd\b'],
            "lob": [r'light\s*of\s*the\s*bhagavata', r'\blob\b'],
            "iso": [r'sri\s*isopani[sṣ]ad', r'\biso\b'],
            "noi": [r'nectar\s*of\s*instruction', r'\bnoi\b'],
            "ttp": [r'transcendental\s*teachings?\s*of\s*prahlad', r'\bttp\b'],
            "nbs": [r'narada\s*bhakti\s*sutra', r'\bnbs\b'],
            "pop": [r'path\s*of\s*perfection', r'\bpop\b']
        }
        
        query_lower = query.lower()
        detected_books = []

        # Check for explicit book requests
        for book_code, patterns in unified_book_patterns.items():
            for pattern in patterns:
                if re.search(pattern, query_lower):
                    # Verify this book exists in the filesystem
                    book_faiss_path = f"faiss/{book_code.lower()}_index.faiss"
                    book_metadata_path = f"book_jsons/{book_code.lower()}_metadata.json"
                    
                    if os.path.exists(book_faiss_path) and os.path.exists(book_metadata_path):
                        detected_books.append(book_code)
                        break
        
        elapsed_time = time.time() - start_time
        if detected_books:
            book_names = [self.book_map.get(b.lower(), b.upper()) for b in detected_books]
            print(f"⏱️ Detected {len(detected_books)} books in {elapsed_time:.2f} seconds: {', '.join(book_names)}")
        else:
            print(f"⏱️ Unified book detection completed in {elapsed_time:.2f} seconds (no specific books found)")
        
        return detected_books
    
    def _process_single_unified_book(self, book_code: str, refined_query: str) -> Optional[Tuple[str, List[Dict], List[Dict]]]:
        """Process a reference to a specific book in the unified collection using dedicated FAISS index"""
        start_time = time.time()
        book_key = book_code.upper()  # Keys in the grouped metadata are uppercase
        
        # Define paths for book-specific files
        book_faiss_path = f"faiss/{book_code.lower()}_index.faiss"
        book_metadata_path = f"book_jsons/{book_code.lower()}_metadata.json"
        
        # Check if we have the necessary files
        if not os.path.exists(book_faiss_path) or not os.path.exists(book_metadata_path):
            print(f"⚠️ Missing files for '{book_code}'")
            return None
        
        # Load the book-specific metadata
        try:
            with open(book_metadata_path, "r") as f:
                book_metadata = json.load(f)
                print(f"✅ Loaded metadata for {book_code}: {len(book_metadata)} entries")
        except Exception as e:
            print(f"⚠️ Error loading metadata for '{book_code}': {str(e)}")
            return None
        
        # Load the book-specific FAISS index
        try:
            book_index = faiss.read_index(book_faiss_path)
            print(f"✅ Loaded FAISS index for {book_code}")
        except Exception as e:
            print(f"⚠️ Error loading FAISS index for '{book_code}': {str(e)}")
            return None
        
        # Get book's full name for display
        book_full_name = self.book_map.get(book_code.lower(), book_code.upper())
        
        # Generate query embedding
        query_vector = model.encode([refined_query])
        
        # Perform search using the book-specific FAISS index
        D, I = book_index.search(query_vector, k=5)  # Get top 5 results
        
        # Process results
        results = []
        for i, idx in enumerate(I[0]):
            if idx < len(book_metadata):
                score = float(D[0][i])
                entry = book_metadata[idx]
                results.append({
                    "score": score,
                    "entry": entry
                })
        
        if not results:
            print(f"⚠️ No relevant results found in {book_code}")
            return None
        
        # Format the top 3 results
        retrieved_context = ""
        retrieval_sources = []
        
        for result in results[:3]:
            entry = result["entry"]
            verse_id = entry.get("id", f"{book_full_name} section")
            text = entry.get("text", "")
            url = entry.get("url", "")
            
            # Format text for this entry
            formatted_text = f"REFERENCE: {verse_id} ({book_full_name})\n\nTEXT: {text}\n\nURL: {url}"
            
            if not retrieved_context:
                retrieved_context = formatted_text
            else:
                retrieved_context += "\n\n---\n\n" + formatted_text
            
            retrieval_sources.append({
                "verse_id": verse_id,
                "book": "unified",
                "specific_book": book_code.lower(),
                "has_text": bool(text),
                "url": url
            })
        
        # Cache the remaining results
        cached_results = []
        if len(results) > 3:
            for result in results[3:]:
                entry = result["entry"]
                verse_id = entry.get("id", f"{book_full_name} section")
                text = entry.get("text", "")
                url = entry.get("url", "")
                
                cached_results.append({
                    "verse_id": verse_id,
                    "book": "unified",
                    "specific_book": book_code.lower(),
                    "text": text[:1000] if text else "",  # Truncate long texts
                    "url": url
                })
        
        elapsed_time = time.time() - start_time
        print(f"⏱️ Book search for {book_code} completed in {elapsed_time:.2f} seconds")
        
        return retrieved_context, retrieval_sources, cached_results
    
    def _process_multiple_unified_books(self, book_codes: List[str], refined_query: str) -> Optional[Tuple[str, List[Dict]]]:
        """Process references to multiple specific books in the unified collection"""
        if not book_codes:
            return None
            
        start_time = time.time()
        print(f"📚 Processing references to {len(book_codes)} books: {', '.join(book_codes)}")
        
        all_contexts = []
        all_sources = []
        cached_results = []
        
        # Process each book
        for book_code in book_codes:
            book_start_time = time.time()
            result = self._process_single_unified_book(book_code, refined_query)
            
            if result:
                context, sources, book_cached = result
                all_contexts.append(context)
                all_sources.extend(sources)
                cached_results.extend(book_cached)
                
                book_elapsed_time = time.time() - book_start_time
                print(f"✅ Added content from {book_code} in {book_elapsed_time:.2f} seconds")
            else:
                print(f"⚠️ No relevant content found in {book_code}")
        
        if not all_contexts:
            elapsed_time = time.time() - start_time
            print(f"⏱️ Multi-book search completed in {elapsed_time:.2f} seconds (no results)")
            return None
        
        # Combine contexts with clear separation
        combined_context = "\n\n===== NEW BOOK =====\n\n".join(all_contexts)
        
        elapsed_time = time.time() - start_time
        print(f"⏱️ Multi-book search completed in {elapsed_time:.2f} seconds with {len(all_sources)} total references")
        
        return combined_context, all_sources, cached_results
    
    def get_more_results(self, cache_id: str) -> Dict[str, Any]:
        """Retrieve additional cached results for a previous query"""
        if cache_id not in self.cached_results:
            return {"error": "No cached results found for this query"}
        
        # Get cached results
        cached = self.cached_results[cache_id]
        
        # Format additional results
        additional_results = []
        
        for source in cached:
            verse_id = source.get('verse_id', 'Unknown reference')
            book_code = source.get('book', '')
            specific_book = source.get('specific_book', '')
            
            if specific_book:
                book_name = self.book_map.get(specific_book, specific_book.upper())
                formatted_result = f"ADDITIONAL REFERENCE: {verse_id} ({book_name})\n\n"
            else:
                formatted_result = f"ADDITIONAL REFERENCE: {verse_id} ({BOOK_CODES.get(book_code, 'Unknown')})\n\n"

            if source.get('verse_text'):
                formatted_result += f"VERSE: {source['verse_text']}\n\n"
            
            if source.get('translation'):
                formatted_result += f"TRANSLATION: {source['translation']}\n\n"
            
            if source.get('purport'):
                formatted_result += f"PURPORT: {source['purport'][:500]}... (truncated)\n\n"
    
            if source.get('text'):  # For unified sources
                formatted_result += f"TEXT: {source['text']}\n\n"
        
            if source.get('url'):
                formatted_result += f"URL: {source['url']}\n\n"
            
            additional_results.append(formatted_result)
        
        return {
            "additional_results": additional_results,
            "count": len(additional_results)
        }
        
    def _process_bg_verse_reference(self, bg_verse_match) -> Tuple[str, List[Dict]]:
        """Process a specific Bhagavad Gita verse reference"""
        start_time = time.time()
        chapter = bg_verse_match.group(1)
        verse = bg_verse_match.group(2)
        verse_pattern = f"Bg. {chapter}.{verse}"
        print(f"🔎 Exact verse requested: {verse_pattern}")
        
        results = []
        for entry in self.metadata["bg"]:
            verse_id = self._extract_verse_id(entry["url"], "bg")
            if verse_id == verse_pattern:
                results.append(entry)

        # If not found, look in unified collection as fallback
        if not results:
            for entry in self.metadata["unified"]:
                verse_id = self._extract_verse_id(entry["url"], "unified")
                if verse_id == verse_pattern:
                    results.append(entry)
        
        if results:
            # Format with clear section markers for source attribution
            verse_entry = results[0]
            book_code = "bg" if "bg" in verse_entry["url"] else "unified"
            verse_id = self._extract_verse_id(verse_entry["url"], "bg")
            verse_part = f"VERSE: {verse_entry.get('verse_text', '')}"
            translation_part = f"TRANSLATION: {verse_entry.get('translation', '')}"
            purport_part = f"PURPORT BY SRILA PRABHUPADA: {verse_entry.get('purport', '')}"
            url_part = f"URL: {verse_entry.get('url', '')}"

            # Store this verse lookup in history
            self._store_verse_lookup(
                book_code, 
                verse_id, 
                {
                    "verse_text": verse_part,
                    "translation": translation_part,
                    "url": url_part
                }
            )
            
            retrieved_context = f"REFERENCE: {verse_id} ({BOOK_CODES[book_code]})\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\n{url_part}"
            
            retrieval_sources = [{
                "verse_id": verse_id,
                "book": book_code,
                "has_verse": 'verse_text' in verse_entry and bool(verse_entry['verse_text']),
                "has_translation": 'translation' in verse_entry and bool(verse_entry['translation']),
                "has_purport": 'purport' in verse_entry and bool(verse_entry['purport']),
                "url": verse_entry.get('url', '')
            }]

            elapsed_time = time.time() - start_time
            print(f"⏱️ BG verse reference search completed in {elapsed_time:.2f} seconds (no results)")

            return retrieved_context, retrieval_sources
            
        elapsed_time = time.time() - start_time
        print(f"⏱️ BG verse reference search completed in {elapsed_time:.2f} seconds (no results)")
        
        return None
    
    def _process_sb_verse_reference(self, sb_verse_match) -> Tuple[str, List[Dict]]:
        """Process a specific Srimad Bhagavatam verse reference"""
        start_time = time.time()
        canto = sb_verse_match.group(1)
        chapter = sb_verse_match.group(2)
        verse = sb_verse_match.group(3)
        verse_pattern = f"SB {canto}.{chapter}.{verse}"
        print(f"🔎 Exact verse requested: {verse_pattern}")
        
        results = []
        for entry in self.metadata["sb"]:
            verse_id = self._extract_verse_id(entry["url"], "sb")
            if verse_id == verse_pattern:
                results.append(entry)

        # If not found, look in unified collection as fallback
        if not results:
            for entry in self.metadata["unified"]:
                verse_id = self._extract_verse_id(entry["url"], "unified")
                if verse_id == verse_pattern:
                    results.append(entry)
        
        if results:
            # Format with clear section markers for source attribution
            verse_entry = results[0]
            book_code = "sb" if "sb" in verse_entry["url"] else "unified"
            verse_id = self._extract_verse_id(verse_entry["url"], book_code)
            verse_part = f"VERSE: {verse_entry.get('verse_text', '')}"
            translation_part = f"TRANSLATION: {verse_entry.get('translation', '')}"
            purport_part = f"PURPORT BY SRILA PRABHUPADA: {verse_entry.get('purport', '')}"
            url_part = f"URL: {verse_entry.get('url', '')}"
            
            # Store this verse lookup in history
            self._store_verse_lookup(
                book_code, 
                verse_id, 
                {
                    "verse_text": verse_part,
                    "translation": translation_part,
                    "url": url_part
                }
            )

            retrieved_context = f"REFERENCE: {verse_id} ({BOOK_CODES[book_code]})\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\n{url_part}"
            
            retrieval_sources = [{
                "verse_id": verse_id,
                "book": book_code,
                "has_verse": 'verse_text' in verse_entry and bool(verse_entry['verse_text']),
                "has_translation": 'translation' in verse_entry and bool(verse_entry['translation']),
                "has_purport": 'purport' in verse_entry and bool(verse_entry['purport']),
                "url": verse_entry.get('url', '')
            }]

            elapsed_time = time.time() - start_time
            print(f"⏱️ SB verse reference search completed in {elapsed_time:.2f} seconds")
            
            return retrieved_context, retrieval_sources
            
        elapsed_time = time.time() - start_time
        print(f"⏱️ SB verse reference search completed in {elapsed_time:.2f} seconds")
        
        return None
    
    def _process_cc_verse_reference(self, cc_verse_match) -> Tuple[str, List[Dict]]:
        """Process a specific Caitanya Caritamrta verse reference"""
        start_time = time.time()
        lila = cc_verse_match.group(1)
        chapter = cc_verse_match.group(2)
        verse = cc_verse_match.group(3)
        
        # Map 'adi', 'madhya', 'antya' to their abbreviations
        lila_map = {"adi": "Adi", "madhya": "Madhya", "antya": "Antya"}
        lila_abbrev = lila_map.get(lila.lower(), lila)
        
        verse_pattern = f"CC {lila_abbrev} {chapter}.{verse}"
        print(f"🔎 Exact verse requested: {verse_pattern}")
        
        results = []
        for entry in self.metadata["cc"]:
            verse_id = self._extract_verse_id(entry["url"], "cc")
            if verse_id == verse_pattern:
                results.append(entry)    
        
        # If not found, look in unified collection as fallback
        if not results:
            for entry in self.metadata["unified"]:
                verse_id = self._extract_verse_id(entry["url"], "unified")
                if verse_id == verse_pattern:
                    results.append(entry)
        
        if results:
            # Format with clear section markers for source attribution
            verse_entry = results[0]
            book_code = "cc" if "cc" in verse_entry["url"] else "unified"
            verse_id = self._extract_verse_id(verse_entry["url"], book_code)
            verse_part = f"VERSE: {verse_entry.get('verse_text', '')}"
            translation_part = f"TRANSLATION: {verse_entry.get('translation', '')}"
            purport_part = f"PURPORT BY SRILA PRABHUPADA: {verse_entry.get('purport', '')}"
            url_part = f"URL: {verse_entry.get('url', '')}"
            
            # Store this verse lookup in history
            self._store_verse_lookup(
                book_code, 
                verse_id, 
                {
                    "verse_text": verse_part,
                    "translation": translation_part,
                    "url": url_part
                }
            )

            retrieved_context = f"REFERENCE: {verse_id} ({BOOK_CODES[book_code]})\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\n{url_part}"
            
            retrieval_sources = [{
                "verse_id": verse_id,
                "book": book_code,
                "has_verse": 'verse_text' in verse_entry and bool(verse_entry['verse_text']),
                "has_translation": 'translation' in verse_entry and bool(verse_entry['translation']),
                "has_purport": 'purport' in verse_entry and bool(verse_entry['purport']),
                "url": verse_entry.get('url', '')
            }]

            elapsed_time = time.time() - start_time
            print(f"⏱️ SB verse reference search completed in {elapsed_time:.2f} seconds")

            return retrieved_context, retrieval_sources
        
        elapsed_time = time.time() - start_time
        print(f"⏱️ CC verse reference search completed in {elapsed_time:.2f} seconds")
        
        return None
    
    def _process_chapter_reference(self, chapter, book_code, refined_query) -> Tuple[str, List[Dict]]:
        """Process a chapter reference for a specific book"""
        start_time = time.time()
        chapter_patterns = None

        # Check for SB canto+chapter pattern (e.g., "SB 4.5" for Canto 4, Chapter 5)
        sb_canto_chapter_match = None
        if book_code == "sb":
            sb_canto_chapter_match = re.search(r'(?:SB)[.\s]*(\d+)[.\s]*(\d+)', refined_query, re.IGNORECASE)
        
        if book_code == "bg":
            chapter_patterns = f"Bg. {chapter}."
        elif book_code == "sb":
            if sb_canto_chapter_match:
                # If both canto and chapter are specified
                canto = sb_canto_chapter_match.group(1)
                chapter = sb_canto_chapter_match.group(2)
                chapter_patterns = f"SB {canto}.{chapter}."
                print(f"🔍 Looking for SB Canto {canto}, Chapter {chapter}")
            else:
                # If only chapter is specified, try various cantos
                chapter_patterns = [f"SB {canto}.{chapter}." for canto in range(1, 11)]
                print(f"🔍 Looking for chapter {chapter} across all cantos: {chapter_patterns}")
        elif book_code == "cc":
            # For CC, we need lila and chapter
            # Since just chapter was mentioned, try various lilas
            chapter_patterns = [
                f"CC Adi {chapter}.", 
                f"CC Madhya {chapter}.", 
                f"CC Antya {chapter}."
            ]
        else:
            # For unified sources, chapter pattern depends on the book
            # This is a simple fallback pattern
            chapter_patterns = f".{chapter}."
        
        results = []
        
        # Search for pattern(s)
        if isinstance(chapter_patterns, list):
            for pattern in chapter_patterns:
                for entry in self.metadata[book_code]:
                    verse_id = self._extract_verse_id(entry["url"], book_code)
                    if pattern in verse_id:
                        results.append(entry)
                if results:
                    break
        else:
            for entry in self.metadata[book_code]:
                verse_id = self._extract_verse_id(entry["url"], book_code)
                # print(f"Checking verse ID: {verse_id} against pattern: {chapter_patterns}")
                if chapter_patterns in verse_id:
                    results.append(entry)
        
        if not results:
            return "", []
        
        # For chapter queries, get the top most relevant verses
        if len(results) > 5:
            # Get verse texts or translations for embedding comparison
            texts = [entry.get('translation', '') or entry.get('verse_text', '') for entry in results]
            query_vector = model.encode([refined_query])
            if texts:
                # Create embeddings
                result_vectors = model.encode(texts)
                
                # Calculate scores
                scores = np.dot(result_vectors, np.array(query_vector).T)
                top_indices = scores.flatten().argsort()[-5:][::-1]
                
                # Keep top results
                results = [results[i] for i in top_indices]
        
        # Generate context from results
        chapter_texts = []
        retrieval_sources = []
        
        for entry in results[:5]:  # Limit to 5 verses for the chapter
            verse_id = self._extract_verse_id(entry["url"], book_code)
            verse_text = entry.get('verse_text', '')
            translation = entry.get('translation', '')
            url = entry.get('url', '')
            
            # Format text for this verse
            verse_part = f"VERSE: {verse_text}" if verse_text else ""
            translation_part = f"TRANSLATION: {translation}" if translation else ""
            
            chapter_texts.append(f"REFERENCE: {verse_id} ({BOOK_CODES[book_code]})\n\n{verse_part}\n\n{translation_part}\n\nURL: {url}")
            
            retrieval_sources.append({
                "verse_id": verse_id,
                "book": book_code,
                "has_verse": bool(verse_text),
                "has_translation": bool(translation),
                "has_purport": False,  # We skip purport in chapter summaries
                "url": url
            })
        
        retrieved_context = "\n\n---\n\n".join(chapter_texts)
        
        # Add chapter summary note
        retrieved_context += f"\n\n(Found {len(results)} verses from chapter {chapter} in {BOOK_CODES[book_code]})"
        
        elapsed_time = time.time() - start_time
        print(f"⏱️ Chapter reference search for {book_code} completed in {elapsed_time:.2f} seconds")

        return retrieved_context, retrieval_sources

    def _process_unified_book_reference(self, book_name: str, refined_query: str) -> Optional[Tuple[str, List[Dict]]]:
        """Process a reference to a specific book in the unified collection using dedicated FAISS index"""
        start_time = time.time()
        # book_key = book_name.upper()  # Keys in the grouped metadata are uppercase
        
        print(f"📖 Processing reference to book: {book_name}")
        
        # Define paths for book-specific files
        book_faiss_path = f"faiss/{book_name.lower()}_index.faiss"
        book_metadata_path = f"book_jsons/{book_name.lower()}_metadata.json"
        
        # Check if we have a dedicated FAISS index for this book
        if not os.path.exists(book_faiss_path):
            print(f"⚠️ No FAISS index found for '{book_name}' at {book_faiss_path}")
            elapsed_time = time.time() - start_time
            print(f"⏱️ Unified book lookup failed in {elapsed_time:.2f} seconds (missing FAISS index)")
            return None
        
        # Load the book-specific metadata
        try:
            with open(book_metadata_path, "r") as f:
                book_metadata = json.load(f)
                print(f"✅ Loaded metadata for {book_name}: {len(book_metadata)} entries")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"⚠️ Error loading metadata for '{book_name}': {str(e)}")
            elapsed_time = time.time() - start_time
            print(f"⏱️ Unified book lookup failed in {elapsed_time:.2f} seconds (metadata error)")
            return None
        
        # Load the book-specific FAISS index
        try:
            book_index = faiss.read_index(book_faiss_path)
            print(f"✅ Loaded FAISS index for {book_name}")
        except Exception as e:
            print(f"⚠️ Error loading FAISS index for '{book_name}': {str(e)}")
            elapsed_time = time.time() - start_time
            print(f"⏱️ Unified book lookup failed in {elapsed_time:.2f} seconds (FAISS loading error)")
            return None
        
        # Get book's full name for display
        book_full_name = self.book_map.get(book_name.lower(), book_name.upper())
        
        # Generate query embedding
        embedding_start = time.time()
        query_vector = model.encode([refined_query])
        
        # Perform search using the book-specific FAISS index
        D, I = book_index.search(query_vector, k=8)  # Get top 8 results
        
        # Process results
        results = []
        for i, idx in enumerate(I[0]):
            if idx < len(book_metadata):
                score = float(D[0][i])
                entry = book_metadata[idx]
                results.append({
                    "score": score,
                    "entry": entry
                })
        
        embedding_time = time.time() - embedding_start
        print(f"⏱️ FAISS search for {book_name} completed in {embedding_time:.2f} seconds with {len(results)} results")
        
        if not results:
            print(f"⚠️ No relevant results found in {book_name}")
            elapsed_time = time.time() - start_time
            print(f"⏱️ Unified book lookup completed in {elapsed_time:.2f} seconds (no relevant results)")
            return None
        
        # Format the top 3 results
        retrieved_context = ""
        retrieval_sources = []
        
        for result in results[:3]:
            entry = result["entry"]
            verse_id = self._extract_verse_id(entry.get("url", ""), "unified") if "url" in entry else f"{book_full_name} (part {entry.get('part', '?')})"
            text = entry.get("text", "")
            url = entry.get("url", "")
            
            # Format text for this entry
            formatted_text = f"REFERENCE: {verse_id} ({book_full_name})\n\nTEXT: {text}\n\nURL: {url}"
            
            if not retrieved_context:
                retrieved_context = formatted_text
            else:
                retrieved_context += "\n\n---\n\n" + formatted_text
            
            retrieval_sources.append({
                "verse_id": verse_id,
                "book": "unified",
                "specific_book": book_name.lower(),
                "has_text": bool(text),
                "url": url
            })
        
        # Cache the remaining results
        cached_results = []
        if len(results) > 3:
            for result in results[3:8]:  # Cache next 5 at most
                entry = result["entry"]
                verse_id = self._extract_verse_id(entry.get("url", ""), "unified") if "url" in entry else f"{book_full_name} (part {entry.get('part', '?')})"
                text = entry.get("text", "")
                url = entry.get("url", "")
                
                cached_results.append({
                    "verse_id": verse_id,
                    "book": "unified",
                    "specific_book": book_name.lower(),
                    "text": text[:1000] if text else "",  # Truncate long texts
                    "url": url
                })
        
        elapsed_time = time.time() - start_time
        print(f"⏱️ Unified book reference search for {book_name} completed in {elapsed_time:.2f} seconds")

        return retrieved_context, retrieval_sources, cached_results
    
    def _semantic_search(self, query, search_books):
        """Perform semantic search across preferred books"""
        print(f"🔍 Performing semantic search across: {search_books}")

        start_time = time.time()
        
        # Generate query embedding
        query_vector = model.encode([query])
        
        # Collect results from each preferred book
        all_results = []
        
        for book_code in search_books:
            if self.indices[book_code] is not None:
                # Get top 5 results from this book
                D, I = self.indices[book_code].search(query_vector, k=5)
                
                book_results = []
                for i in I[0]:
                    if i < len(self.metadata[book_code]):
                        entry = self.metadata[book_code][i]
                        verse_id = self._extract_verse_id(entry["url"], book_code)
                        
                        book_results.append({
                            "score": float(D[0][len(book_results)]),  # Match score with result
                            "index": i,
                            "book_code": book_code,
                            "verse_id": verse_id,
                            "entry": entry
                        })
                
                all_results.extend(book_results)
        
        # Sort all results by score
        all_results.sort(key=lambda x: x["score"], reverse=True)
        
        # Top 3 for immediate use
        top_results = all_results[:3]
        
        # Next 5 for cache
        cached_results = []
        if len(all_results) > 3:
            cached_results = all_results[3:8]
        
        # Format top results for context
        retrieved_context = ""
        retrieval_sources = []
        
        for result in top_results:
            book_code = result["book_code"]
            entry = result["entry"]
            verse_id = result["verse_id"]
            
            # Use pre-formatted metadata if available
            idx = result["index"]
            if idx < len(self.formatted_metadata[book_code]):
                formatted_text = self.formatted_metadata[book_code][idx]
                if not retrieved_context:
                    retrieved_context = formatted_text
                else:
                    retrieved_context += "\n\n---\n\n" + formatted_text
            else:
                # Fall back to manual formatting
                verse_text = entry.get('verse_text', '')
                translation = entry.get('translation', '')
                purport = entry.get('purport', '')
                url = entry.get('url', '')
                
                verse_part = f"VERSE: {verse_text}" if verse_text else ""
                translation_part = f"TRANSLATION: {translation}" if translation else ""
                purport_part = f"PURPORT BY SRILA PRABHUPADA: {purport}" if purport else ""
                
                formatted_text = f"REFERENCE: {verse_id} ({BOOK_CODES[book_code]})\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\nURL: {url}"
                
                if not retrieved_context:
                    retrieved_context = formatted_text
                else:
                    retrieved_context += "\n\n---\n\n" + formatted_text
            
            # Add to retrieval sources
            retrieval_sources.append({
                "verse_id": verse_id,
                "book": book_code,
                "has_verse": 'verse_text' in entry and bool(entry['verse_text']),
                "has_translation": 'translation' in entry and bool(entry['translation']),
                "has_purport": 'purport' in entry and bool(entry['purport']),
                "url": entry.get('url', '')
            })
        
        # Format cached results
        cached_formatted = []
        for result in cached_results:
            entry = result["entry"]
            verse_id = result["verse_id"]
            book_code = result["book_code"]
            
            cached_formatted.append({
                "verse_id": verse_id,
                "book": book_code,
                "verse_text": entry.get('verse_text', ''),
                "translation": entry.get('translation', ''),
                "purport": entry.get('purport', '')[:1000] if 'purport' in entry else '',  # Truncate long purports
                "text": entry.get('text', '')[:1000] if 'text' in entry else '',  # Added for unified sources
                "url": entry.get('url', '')
            })
        
        elapsed_time = time.time() - start_time
        print(f"⏱️ Semantic search completed in {elapsed_time:.2f} seconds with {len(top_results)} top results")

        return retrieved_context, retrieval_sources, cached_formatted
    
    def _store_verse_lookup(self, book_code: str, verse_id: str, data: Dict) -> None:
        """
        Store verse lookup information in a persistent JSON file
        
        Args:
            book_code: The book code (bg, sb, cc)
            verse_id: The verse identifier (e.g., Bg. 2.45)
            data: Dictionary containing verse info (text, translation, etc.)
        """
        # Ensure the directory exists
        os.makedirs("history", exist_ok=True)
        history_file = "history/verse_lookups.json"
        
        # Get current timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Prepare entry
        entry = {
            "timestamp": timestamp,
            "book_code": book_code,
            "verse_id": verse_id,
            "verse_text": data.get("verse_text", ""),
            "translation": data.get("translation", ""),
            "url": data.get("url", "")
        }
        
        # Load existing history or create new
        try:
            if os.path.exists(history_file):
                with open(history_file, "r") as f:
                    history = json.load(f)
            else:
                history = []
        except (json.JSONDecodeError, FileNotFoundError):
            # If file is corrupted or doesn't exist, start fresh
            history = []
        
        # Add new entry
        history.append(entry)
        
        # Save updated history
        try:
            with open(history_file, "w") as f:
                json.dump(history, f, indent=2)
            print(f"✅ Stored verse lookup for {verse_id}")
        except Exception as e:
            print(f"⚠️ Failed to store verse lookup: {str(e)}")
    
    def _get_facts_extraction_prompt(self, devotee_level: str) -> str:
        """Get the facts extraction prompt based on devotee level"""
        base_prompt = (
            "You are an expert on Srila Prabhupada's teachings. Your job is to carefully extract important information from the context "
            "with precise attribution and URLs. Follow these guidelines:\n\n"
            
            "1. BEGIN with a one-sentence acknowledgment of Srila Prabhupada's contribution.\n\n"
            
            "2. Extract 3-5 key facts or teachings that address the user's question.  <- <- User's question to be answered perfectly.\n\n"
            
            "3. For each fact:\n"
            "   - Include the exact reference ID (e.g., Bg. 2.47, SB 1.1.1, etc.)\n"
            "   - Include direct quotes from the verse, translation, or purport\n"
            "   - Specify the source (verse, translation or purport)\n"
            "   - Keep the original wording intact\n"
            "   - Include the URL for the verse\n"
            
            "4. Format each fact as: \"FACT: [brief description] | SOURCE: [reference and type] | QUOTE: \"[exact quote]\" | URL: [clickable link]\"\n\n"
            
            "5. ONLY include information directly from the provided context.\n\n"
            
            "6. Do NOT interpret or expand beyond what's explicitly stated in the context.\n\n"
        )
        
        # Add devotee level specific instructions
        if devotee_level == "neophyte":
            base_prompt += (
                "7. Since the user is a neophyte devotee, focus on extracting the most basic and fundamental teachings. "
                "Prioritize clear, simple explanations from Srila Prabhupada that establish core principles.\n\n"
            )
        elif devotee_level == "advanced":
            base_prompt += (
                "7. Since the user is an advanced devotee, include more nuanced philosophical points and deeper purport explanations. "
                "Don't shy away from complex Sanskrit terms and philosophical concepts that Prabhupada elaborates on.\n\n"
            )
        
        return base_prompt
    
    def _get_user_friendly_prompt(self, answer_format: str, devotee_level: str, prabhupada_ratio: int) -> str:
        """Get the user-friendly transformation prompt based on format/level/ratio"""
        # Base prompt structure
        base_prompt = (
            "You are an expert on Srila Prabhupada's teachings who transforms factual information into explanations "
            "for devotees. Follow these guidelines:\n\n"
        )
        
        # Add format-specific instructions
        if answer_format == "conversational":
            base_prompt += (
                "1. START with a humble greeting that acknowledges Srila Prabhupada once at the beginning.\n\n"
                
                "2. TRANSFORM the extracted facts into a cohesive, conversational explanation that feels personal.\n\n"
                
                "3. ORGANIZE the content with clear visual structure:\n"
                "   - Use elegant section headings (marked with 🕉 or ✨)\n"
                "   - Highlight key concepts in *bold*\n"
                "   - Use thoughtful paragraph breaks for readability\n\n"
                
                "4. USE language that feels warm, direct, and instructive - as if speaking directly to the devotee.\n\n"
            )
        elif answer_format == "academic":
            base_prompt += (
                "1. START with a brief introduction to the topic without personal greetings.\n\n"
                
                "2. TRANSFORM the extracted facts into a structured, scholarly analysis with logical progression.\n\n"
                
                "3. ORGANIZE content with academic structure:\n"
                "   - Use numbered or titled sections\n"
                "   - Present arguments in logical sequence\n"
                "   - Cite sources with academic precision\n\n"
                
                "4. USE language that is precise, objective, and scholarly but still accessible.\n\n"
            )
        elif answer_format == "scriptural":
            base_prompt += (
                "1. START with a traditional Sanskrit invocation or verse reference.\n\n"
                
                "2. TRANSFORM the extracted facts into a traditional commentary style reminiscent of Vedic literature.\n\n"
                
                "3. ORGANIZE content with scriptural structure:\n"
                "   - Use formal section divisions\n"
                "   - Present arguments in traditional order\n"
                "   - Reference related scriptural passages\n\n"
                
                "4. USE language that is reverent, formal, and rich with traditional terminology.\n\n"
            )
        elif answer_format == "bullet-points":
            base_prompt += (
                "1. START with a brief introduction to the topic.\n\n"
                
                "2. TRANSFORM the extracted facts into organized bullet points for easy reading.\n\n"
                
                "3. ORGANIZE content with clear structure:\n"
                "   - Use main bullet points for primary ideas\n"
                "   - Use sub-bullets for supporting details\n"
                "   - Group related concepts together under headings\n\n"
                
                "4. USE language that is concise, clear, and direct.\n\n"
            )
        
        elif answer_format == "lecture":
            base_prompt += (
                "1. STRUCTURE the content like a live lecture or training session.\n\n"
                "2. BEGIN with a **Session Title** (e.g., 'Session 1: The Executive's Dharma – Finding Your Leadership Purpose').\n\n"
                "3. INCLUDE a section called **Gita Reference** to list relevant verses.\n\n"
                "4. USE the following flow in organization:\n"
                "   - **Opening Hook** with a real-life case study and compelling Prabhupada quote\n"
                "   - **Core Teaching**: Deep dive into the topic with scriptural explanations, examples, and Prabhupada's purports\n"
                "   - **Frameworks or Levels**: Present key philosophical distinctions (e.g., tamasic/rajasic/sattvic leadership)\n"
                "   - **Practical Application**: Activities like personal audits, action items, reflections\n"
                "   - **Micro-practice**: Include simple daily practices for implementation\n"
                "   - **Closing Reflection**: Reinforce the session’s message with a final quote\n"
                "   - **Key Quotes** and **Summary** section at the end\n\n"
                "5. USE engaging, instructional language, suitable for a session or workshop facilitator.\n\n"
            )

        # Add devotee level specific instructions
        if devotee_level == "neophyte":
            base_prompt += (
                "5. For NEOPHYTE devotees:\n"
                "   - Explain basic philosophical terms when they appear\n"
                "   - Use simple analogies and examples\n"
                "   - Focus on practical applications of the teachings\n"
                "   - Avoid assuming familiarity with complex concepts\n\n"
            )
        elif devotee_level == "intermediate":
            base_prompt += (
                "5. For INTERMEDIATE devotees:\n"
                "   - Assume basic familiarity with key concepts\n"
                "   - Provide moderate depth in explanations\n"
                "   - Balance philosophical depth with practical application\n"
                "   - Connect teachings to broader philosophical frameworks\n\n"
            )
        elif devotee_level == "advanced":
            base_prompt += (
                "5. For ADVANCED devotees:\n"
                "   - Assume substantial knowledge of Vaishnava philosophy\n"
                "   - Don't shy away from Sanskrit terminology\n"
                "   - Explore deeper philosophical implications\n"
                "   - Connect to more obscure or advanced texts and concepts\n\n"
            )
        
        # Handle Prabhupada ratio
        base_prompt += (
            f"6. INCLUDE DIRECT QUOTES based on a {prabhupada_ratio}% Prabhupada content ratio:\n"
            f"   - {'Use extensive direct quotes throughout' if prabhupada_ratio >= 70 else 'Balance direct quotes with explanation'}\n"
            f"   - Format quotes with quotation marks and include the reference\n"
            f"   - Add references as clickable links [Source](URL)\n\n"
        )
        
        # Final instructions
        base_prompt += (
            "7. MAINTAIN complete accuracy to the source material - don't add new interpretations.\n\n"
            
            "8. CONCLUDE with a practical application or reflection.\n\n"
            
            "Your goal is to create a reading experience that maintains fidelity to the original teachings."
        )
        
        return base_prompt
    
    def _get_token_length(self, answer_length: str, stage: str) -> int:
        """Get appropriate token limit based on desired answer length"""
        if stage == "facts":
            # Facts extraction stage
            token_map = {"short": 1000, "medium": 1600, "long": 2500}
        else:
            # Final answer stage
            token_map = {"short": 600, "medium": 1200, "long": 2000}
        
        return token_map.get(answer_length, 800)
    
    def _calculate_prabhupada_ratio(self, final_answer: str, extracted_facts: str) -> int:
        """Calculate the approximate percentage of Prabhupada's words in the answer"""
        # Extract all quotes from the final answer
        quotes = re.findall(r'"([^"]+)"', final_answer)
        
        # Calculate total length
        total_length = len(final_answer)
        quote_length = sum(len(q) for q in quotes)
        
        # Calculate ratio (prevent division by zero)
        if total_length > 0:
            ratio = (quote_length / total_length) * 100
            return round(ratio)
        return 0
    
    def _save_query_results(self, original_query, refined_query, retrieved_context, extracted_facts, final_answer, retrieval_sources, actual_ratio, time_):
        """Save detailed query results analysis for each step of the RAG pipeline"""
        import datetime
        
        # Create timestamped file for better tracking of multiple queries
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        results_path = f"testing/rag_results_{timestamp}.txt"
        
        with open(results_path, "w") as f:
            # Title and overview
            f.write("=" * 80 + "\n")
            f.write(f"PRABHUPADA RAG SYSTEM ANALYSIS RESULTS - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            # Step 1: Query processing
            f.write("1️⃣ QUERY PROCESSING\n")
            f.write("-" * 80 + "\n")
            f.write(f"Original Query: {original_query}\n")
            f.write(f"Refined Query: {refined_query}\n\n")
            
            # Step 2: Book detection and source selection
            f.write("2️⃣ SOURCE SELECTION\n")
            f.write("-" * 80 + "\n")
            book_sources = [source['book'] for source in retrieval_sources]
            unique_books = set(book_sources)
            f.write(f"Detected Sources: {', '.join(unique_books)}\n")
            
            # Special handling for unified book
            specific_books = [source.get('specific_book', '') for source in retrieval_sources]
            specific_books = [b for b in specific_books if b]
            if specific_books:
                book_names = [self.book_map.get(b, b.upper()) for b in specific_books]
                f.write(f"Specific Books from Unified Collection: {', '.join(book_names)}\n")
            f.write("\n")
            
            # Step 3: Retrieved contexts
            f.write("3️⃣ RETRIEVED CONTEXT\n")
            f.write("-" * 80 + "\n")
            f.write(f"Retrieved {len(retrieval_sources)} source(s)\n\n")
            f.write(retrieved_context + "\n\n")
            
            # Step 4: Fact extraction
            f.write("4️⃣ FACT EXTRACTION\n")
            f.write("-" * 80 + "\n")
            f.write(extracted_facts + "\n\n")
            
            # Step 5: Final answer generation
            f.write("5️⃣ FINAL ANSWER\n")
            f.write("-" * 80 + "\n")
            f.write(final_answer + "\n\n")
            
            # Step 6: Metrics and analysis
            f.write("6️⃣ METRICS AND ANALYSIS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Target Prabhupada Content Ratio: {self.last_prabhupada_ratio}%\n")
            f.write(f"Actual Prabhupada Content Ratio: {actual_ratio}%\n")
            f.write(f"Answer Length Setting: {self.last_answer_length}\n")
            f.write(f"Answer Format: {self.last_answer_format}\n")
            f.write(f"Devotee Level: {self.last_devotee_level}\n\n")
            
            # Step 7: Detailed source citations
            f.write("7️⃣ DETAILED SOURCE CITATIONS\n")
            f.write("-" * 80 + "\n")
            for i, source in enumerate(retrieval_sources):
                book_code = source.get('book', 'Unknown')
                book_name = BOOK_CODES.get(book_code, 'Unknown')
                
                # Add specific book info for unified sources
                if book_code == "unified" and source.get('specific_book'):
                    specific_book = source.get('specific_book')
                    book_name = f"{book_name} - {self.book_map.get(specific_book, specific_book.upper())}"
                    
                f.write(f"Source #{i+1}: {source['verse_id']} ({book_name})\n")
                f.write(f"  URL: {source.get('url', 'N/A')}\n")
                
                # Content details
                f.write(f"  Content Types: ")
                content_types = []
                if source.get('has_verse'):
                    content_types.append("Verse ✓")
                if source.get('has_translation'):
                    content_types.append("Translation ✓")
                if source.get('has_purport'):
                    content_types.append("Purport ✓")
                if source.get('has_text'):
                    content_types.append("Text ✓")
                if not content_types:
                    content_types = ["No content details available"]
                f.write(", ".join(content_types) + "\n\n")
            
            # Add performance metrics section
            f.write("8️⃣ PERFORMANCE METRICS\n")
            f.write("-" * 80 + "\n")
            f.write(f"Search Time: {time_['search_elapsed_time']:.2f} seconds\n")
            f.write(f"Total Processing Time: {time_['overall_elapsed_time']:.2f} seconds\n")

            # Summary
            f.write("\n" + "=" * 80 + "\n")
            f.write("ANALYSIS SUMMARY\n")
            f.write("=" * 80 + "\n")
            f.write(f"- Query successfully processed through {len(unique_books)} source(s)\n")
            f.write(f"- Retrieved {len(retrieval_sources)} relevant passages\n")
            f.write(f"- Final answer contains approximately {actual_ratio}% of Prabhupada's direct words\n")
            f.write(f"- Results saved to: {results_path}\n")
            
        print(f"\n✅ Detailed analysis saved to {results_path}")
        return results_path
    
# Sample use
if __name__ == "__main__":
    rag_system = PrabhupadaRAG()
    
    # Example: ask a question about karma yoga
    query = "glorify the supreme lord krsna from teaching from sb canto 7 chapter 6"
    
    # Example customizations (these would come from user preferences in real app)
    prabhupada_ratio = 0  # 70% of content should be direct quotes
    answer_length = "long"  # short, medium, long
    answer_format = "conversational"  # conversational, academic, scriptural, bullet-points, lecture
    devotee_level = "intermediate"  # neophyte, intermediate, advanced
    
    result = rag_system.process_query(
        original_query=query,
        # preferred_books=preferred_books,
        prabhupada_ratio=prabhupada_ratio,
        answer_length=answer_length,
        answer_format=answer_format,
        devotee_level=devotee_level
    )
    
    # Get more results if needed
    follow_up = input("\nWould you like more information on this topic? (y/n): ")
    if follow_up.lower() == 'y':
        more_results = rag_system.get_more_results(result["cache_id"])
        print("\nAdditional references:")
        for result in more_results.get("additional_results", []):
            print(f"\n{result}")