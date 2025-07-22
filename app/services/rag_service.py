# rag_service.py
import sys
import os
import re
from typing import Dict, List, Any, Optional
import json
from sqlalchemy.orm import Session
from groq import Groq

client = Groq(api_key=os.environ.get("GROQ_API_KEY", "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"))

with open("gita_metadata.json", "r") as f:
    structured_metadata = json.load(f)

# Add parent directory to path to import rag_testing
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
import rag_testing

from .. import models, schemas

class RAGService:
    def __init__(self):
        # Your existing RAG setup is already in rag_testing.py
        pass
    
    def process_query_with_session(
        self, 
        query: str, 
        session_token: str,
        custom_ratio: Optional[int] = None,
        custom_length: Optional[str] = None,
        custom_format: Optional[str] = None,
        selected_books: Optional[List[str]] = None,
        db: Session
    ) -> Dict[str, Any]:
        # Get session data
        db_session = db.query(models.Session).filter(
            models.Session.session_token == session_token
        ).first()
        
        if not db_session:
            raise ValueError("Invalid session token")
        
        # Get user preferences
        user_prefs = db.query(models.UserPreference).filter(
            models.UserPreference.user_id == db_session.user_id
        ).first()
        
        if not user_prefs:
            # Create default preferences if none exist
            user_prefs = models.UserPreference(user_id=db_session.user_id)
            db.add(user_prefs)
            db.commit()
        
        # Get source preferences
        source_prefs = db.query(models.SourcePreference).filter(
            models.SourcePreference.user_id == db_session.user_id
        ).first()
        
        if not source_prefs:
            # Create default source preferences if none exist
            source_prefs = models.SourcePreference(user_id=db_session.user_id)
            db.add(source_prefs)
            db.commit()
        
        # Get last verses read by user and bookmarks (unchanged)
        verse_histories = db.query(models.VerseHistory).filter(
            models.VerseHistory.session_id == db_session.id
        ).order_by(models.VerseHistory.timestamp.desc()).limit(5).all()
        
        last_verses = [vh.verse_id for vh in verse_histories]
        
        user_memories = db.query(models.VerseMemory).filter(
            models.VerseMemory.user_id == db_session.user_id,
            models.VerseMemory.bookmarked == True
        ).all()
        
        bookmarked_verses = [vm.verse_id for vm in user_memories]
        
        # Use selected preferences for this query or default to user preferences
        prabhupada_ratio = custom_ratio if custom_ratio is not None else user_prefs.prabhupada_ratio
        answer_length = custom_length if custom_length else user_prefs.preferred_answer_length
        answer_format = custom_format if custom_format else user_prefs.preferred_format
        devotee_level = user_prefs.devotee_level
        
        # Handle source selection for this query
        if selected_sources:
            source_config = {"selected_sources": selected_sources}
        else:
            source_config = {
                "bg_enabled": source_prefs.bg_enabled,
                "sb_enabled": source_prefs.sb_enabled,
                "cc_enabled": source_prefs.cc_enabled,
                "other_books_enabled": source_prefs.other_books_enabled,
                "specific_books": source_prefs.specific_books,
                "lectures_enabled": source_prefs.lectures_enabled,
                "letters_enabled": source_prefs.letters_enabled,
                "conversations_enabled": source_prefs.conversations_enabled
            }
        
        # Enhance query with session context
        enhanced_query = self._enhance_query_with_context(
            query, 
            last_verses, 
            bookmarked_verses, 
            db_session.streak_days
        )
        
        # Process the query with customizations
        rag_result = rag_testing.process_query(
            enhanced_query,
            prabhupada_ratio=prabhupada_ratio,
            answer_length=answer_length,
            answer_format=answer_format,
            devotee_level=devotee_level,
            source_config=source_config
        )
        
        # Extract verse references and update history
        verse_refs = self._extract_verse_references(rag_result["final_answer"])
        
        if verse_refs:
            most_specific_verse = verse_refs[0]
            self._add_verse_to_history(most_specific_verse, db_session.id, db)
        
        # Update session's last query
        db_session.last_query = query
        db.commit()
        
        return {
            "answer": rag_result["final_answer"],
            "retrieved_verses": verse_refs,
            "last_verse": most_specific_verse if verse_refs else None,
            "streak_days": db_session.streak_days,
            "prabhupada_ratio": prabhupada_ratio,  # Include ratio in response
            "sources_used": rag_result.get("sources_used", [])  # Include sources used
        }
    
    def _detect_question_format(self, query: str) -> str:
        """Detect if the query is a specific question format."""
        query_lower = query.lower()
        
        # True/False detection
        if "true or false" in query_lower or "true/false" in query_lower:
            return "true_false"
        
        # MCQ detection
        mcq_patterns = [
            r'(?:which|what).*?\ba\)\s.*?\bb\)\s',
            r'\b[a-d]\)\s.*?\b[a-d]\)\s',
            r'multiple choice'
        ]
        for pattern in mcq_patterns:
            if re.search(pattern, query_lower):
                return "mcq"
        
        # Fill in the blank detection
        if "fill in the blank" in query_lower or "___" in query or "..." in query:
            return "fill_blank"
        
        # Matching detection
        if "match the following" in query_lower or "matching" in query_lower:
            return "matching"
        
        return "general"
    
    def _enhance_query_with_context(
        self, 
        query: str, 
        last_verses: List[str],
        bookmarked_verses: List[str],
        streak_days: int
    ) -> str:
        """Enhance the user query with session context."""
        context_parts = []
        
        # Add reading streak context
        if streak_days > 1:
            context_parts.append(f"The user has been studying for {streak_days} consecutive days.")
        
        # Add recently read verses context
        if last_verses:
            verses_str = ", ".join(last_verses[:3])
            context_parts.append(f"The user has recently read these verses: {verses_str}.")
        
        # Add bookmarked verses context if relevant to query
        if bookmarked_verses and any(v.lower() in query.lower() for v in bookmarked_verses):
            relevant_bookmarks = [v for v in bookmarked_verses if v.lower() in query.lower()]
            if relevant_bookmarks:
                bookmarks_str = ", ".join(relevant_bookmarks)
                context_parts.append(f"The user has bookmarked these relevant verses: {bookmarks_str}.")
        
        # If we have context to add, combine it with the original query
        if context_parts:
            context_str = " ".join(context_parts)
            enhanced_query = f"{query} [SESSION CONTEXT: {context_str}]"
            return enhanced_query
        
        return query
    
    def _extract_verse_references(self, text: str) -> List[str]:
        """Extract verse references from response text."""
        verse_pattern = r'Bg\.\s*(\d+)\.(\d+)'
        matches = re.findall(verse_pattern, text)
        
        # Format matches as "Bg. X.Y"
        verse_refs = [f"Bg. {match[0]}.{match[1]}" for match in matches]
        return verse_refs
    
    def _add_verse_to_history(self, verse_id: str, session_id: int, db: Session):
        """Add a verse to the user's history."""
        history_entry = models.VerseHistory(
            session_id=session_id,
            verse_id=verse_id
        )
        db.add(history_entry)
        db.commit()

    def process_prompt_directly(self, prompt: str) -> str:
        """Process a prompt directly with the LLM without RAG retrieval."""
        completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": "You are a knowledgeable assistant specializing in the Bhagavad Gita."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            model="llama3-70b-8192",
            temperature=0.4,
            max_tokens=1000
        )
        
        return completion.choices[0].message.content

    def retrieve_chapter_content(self, chapter: int) -> dict:
        """Retrieve content for a specific chapter."""
        # Filter metadata by chapter
        chapter_verses = [entry for entry in structured_metadata 
                        if entry.get('verse_id', '').startswith(f"Bg. {chapter}.")]
        
        if not chapter_verses:
            return {"content": f"No content found for Chapter {chapter}"}
        
        # Format verses with clear section markers
        chapter_texts = []
        for entry in chapter_verses:
            verse_id = entry.get('verse_id', 'Unknown verse')
            verse_text = entry.get('verse_text', '')
            translation = entry.get('translation', '')
            purport = entry.get('purport', '')
            url = entry.get('url', '')
            
            formatted_text = f"REFERENCE: {verse_id}\n\nVERSE: {verse_text}\n\n"
            formatted_text += f"TRANSLATION: {translation}\n\n"
            
            if purport:
                formatted_text += f"PURPORT BY SRILA PRABHUPADA: {purport}\n\n"
                
            formatted_text += f"URL: {url}"
            chapter_texts.append(formatted_text)
        
        combined_text = "\n\n---\n\n".join(chapter_texts)
        combined_text += f"\n\n(Chapter {chapter} has {len(chapter_verses)} verses in total)"
        
        return {
            "content": combined_text,
            "verse_count": len(chapter_verses),
            "chapter": chapter
        }