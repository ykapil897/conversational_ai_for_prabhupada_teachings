# models.py
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import datetime

from .database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    sessions = relationship("Session", back_populates="user")
    verse_memories = relationship("VerseMemory", back_populates="user")

class Session(Base):
    __tablename__ = "sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    session_token = Column(String, unique=True, index=True)
    started_at = Column(DateTime, default=func.now())
    last_active_at = Column(DateTime, default=func.now(), onupdate=func.now())
    streak_days = Column(Integer, default=1)
    last_query = Column(Text, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="sessions")
    verse_history = relationship("VerseHistory", back_populates="session")

class VerseHistory(Base):
    __tablename__ = "verse_history"
    
    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(Integer, ForeignKey("sessions.id"))
    verse_id = Column(String) # e.g., "Bg. 2.47"
    timestamp = Column(DateTime, default=func.now())
    
    # Relationships
    session = relationship("Session", back_populates="verse_history")

class VerseMemory(Base):
    __tablename__ = "verse_memories"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    verse_id = Column(String) # e.g., "Bg. 2.47"
    bookmarked = Column(Boolean, default=False)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    # Relationships
    user = relationship("User", back_populates="verse_memories")

class Quiz(Base):
    __tablename__ = "quizzes"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    quiz_type = Column(String)  # "mcq" or "cloze"
    source_type = Column(String)  # "purport" or "verse" or "translation"
    chapter = Column(Integer, nullable=True)
    verse_range = Column(String, nullable=True)  # e.g., "1-10" or "all"
    created_at = Column(DateTime, default=func.now())
    completed_at = Column(DateTime, nullable=True)
    score = Column(Integer, nullable=True)
    max_score = Column(Integer, nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="quizzes")
    questions = relationship("QuizQuestion", back_populates="quiz")

class QuizQuestion(Base):
    __tablename__ = "quiz_questions"
    
    id = Column(Integer, primary_key=True, index=True)
    quiz_id = Column(Integer, ForeignKey("quizzes.id"))
    question_text = Column(Text)
    question_type = Column(String)  # "mcq" or "cloze"
    verse_id = Column(String)  # e.g., "Bg. 2.47"
    options = Column(Text)  # JSON string of options
    correct_answer = Column(Text)
    user_answer = Column(Text, nullable=True)
    is_correct = Column(Boolean, nullable=True)
    
    # Relationships
    quiz = relationship("Quiz", back_populates="questions")

# Update the User model to include relationship with quizzes
User.quizzes = relationship("Quiz", back_populates="user")