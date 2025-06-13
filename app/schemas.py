# schemas.py
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from datetime import datetime

class UserBase(BaseModel):
    username: str
    email: EmailStr

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