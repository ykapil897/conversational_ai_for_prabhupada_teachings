# schemas.py
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from datetime import datetime

class UserBase(BaseModel):
    username: str
    email: EmailStr
    selected_books: List[str] = ["bg"] 

class UserCreate(UserBase):
    pass

class User(UserBase):
    id: int
    created_at: datetime
    
    class Config:
        orm_mode = True

class SessionBase(BaseModel):
    user_id: int

class SessionCreate(SessionBase):
    pass

class Session(SessionBase):
    id: int
    session_token: str
    started_at: datetime
    last_active_at: datetime
    streak_days: int
    last_query: Optional[str] = None
    
    class Config:
        orm_mode = True

class VerseHistoryBase(BaseModel):
    session_id: int
    verse_id: str

class VerseHistoryCreate(VerseHistoryBase):
    pass

class VerseHistory(VerseHistoryBase):
    id: int
    timestamp: datetime
    
    class Config:
        orm_mode = True

class VerseMemoryBase(BaseModel):
    user_id: int
    verse_id: str
    bookmarked: bool = False
    notes: Optional[str] = None

class VerseMemoryCreate(VerseMemoryBase):
    pass

class VerseMemory(VerseMemoryBase):
    id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True

class QueryRequest(BaseModel):
    query: str
    session_token: str
    custom_ratio: Optional[int] = None  # Override user's default ratio for this query
    custom_length: Optional[str] = None  # Override user's default length for this query
    custom_format: Optional[str] = None  # Override user's default format for this query
    selected_books: Optional[List[str]] = None  # Override source selection for this query

class QueryResponse(BaseModel):
    answer: str
    retrieved_verses: List[str]
    last_verse: Optional[str] = None
    streak_days: int

class QuizQuestionBase(BaseModel):
    question_text: str
    question_type: str
    verse_id: str
    options: str  # JSON string of options
    correct_answer: str

class QuizQuestionCreate(QuizQuestionBase):
    pass

class QuizQuestion(QuizQuestionBase):
    id: int
    quiz_id: int
    user_answer: Optional[str] = None
    is_correct: Optional[bool] = None
    
    class Config:
        orm_mode = True

class QuizBase(BaseModel):
    user_id: int
    quiz_type: str
    source_type: str
    chapter: Optional[int] = None
    verse_range: Optional[str] = None

class QuizCreate(QuizBase):
    pass

class Quiz(QuizBase):
    id: int
    created_at: datetime
    completed_at: Optional[datetime] = None
    score: Optional[int] = None
    max_score: Optional[int] = None
    questions: List[QuizQuestion] = []
    
    class Config:
        orm_mode = True

class QuizStartRequest(BaseModel):
    user_id: int
    quiz_type: str  # "mcq" or "cloze"
    source_type: str  # "purport" or "verse" or "translation"
    chapter: Optional[int] = None
    verse_range: Optional[str] = None
    num_questions: int = 5

class QuizSubmitAnswer(BaseModel):
    quiz_id: int
    question_id: int
    user_answer: str

class QuizSubmitRequest(BaseModel):
    quiz_id: int
    answers: List[QuizSubmitAnswer]

class UserPreferenceBase(BaseModel):
    devotee_level: str = "intermediate"  # neophyte, intermediate, advanced
    prabhupada_ratio: int = 70  # percentage of original content
    preferred_answer_length: str = "medium"  # short, medium, long
    preferred_format: str = "conversational"  # conversational, academic, scriptural

class UserPreferenceCreate(UserPreferenceBase):
    user_id: int

class UserPreference(UserPreferenceBase):
    id: int
    user_id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True

class SourcePreferenceBase(BaseModel):
    bg_enabled: bool = True
    sb_enabled: bool = True
    cc_enabled: bool = True
    other_books_enabled: bool = True
    specific_books: Optional[str] = None  # JSON list of specific books
    lectures_enabled: bool = True
    letters_enabled: bool = True
    conversations_enabled: bool = True

class SourcePreferenceCreate(SourcePreferenceBase):
    user_id: int

class SourcePreference(SourcePreferenceBase):
    id: int
    user_id: int
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True