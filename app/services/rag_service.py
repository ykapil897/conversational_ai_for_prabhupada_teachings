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
        db: Session
    ) -> Dict[str, Any]:
        # Get session data
        db_session = db.query(models.Session).filter(
            models.Session.session_token == session_token
        ).first()
        
        if not db_session:
            raise ValueError("Invalid session token")
        
        # Get last verses read by user
        verse_histories = db.query(models.VerseHistory).filter(
            models.VerseHistory.session_id == db_session.id
        ).order_by(models.VerseHistory.timestamp.desc()).limit(5).all()
        
        last_verses = [vh.verse_id for vh in verse_histories]
        
        # Get user's verse memories and bookmarks
        user_memories = db.query(models.VerseMemory).filter(
            models.VerseMemory.user_id == db_session.user_id,
            models.VerseMemory.bookmarked == True
        ).all()
        
        bookmarked_verses = [vm.verse_id for vm in user_memories]
        
        # Enhance query with session context
        enhanced_query = self._enhance_query_with_context(
            query, 
            last_verses, 
            bookmarked_verses, 
            db_session.streak_days
        )
        
        # Use the enhanced query with your existing RAG system
        rag_result = rag_testing.process_query(enhanced_query)
        
        # Extract verse references from the response
        verse_refs = self._extract_verse_references(rag_result["final_answer"])
        
        # If we found verse references, add the most specific one to the user's verse history
        if verse_refs:
            most_specific_verse = verse_refs[0]  # Usually the first mentioned is most relevant
            self._add_verse_to_history(most_specific_verse, db_session.id, db)
        
        # Update session's last query
        db_session.last_query = query
        db.commit()
        
        return {
            "answer": rag_result["final_answer"],
            "retrieved_verses": verse_refs,
            "last_verse": most_specific_verse if verse_refs else None,
            "streak_days": db_session.streak_days
        }
    
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

    # ... existing code ...

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