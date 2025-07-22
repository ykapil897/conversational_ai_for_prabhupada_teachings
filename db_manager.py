from sqlalchemy import create_engine, Column, Integer, String, Text, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import json

DB_PATH = "devotee_chatbot.db"
Base = declarative_base()
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False)
SessionLocal = sessionmaker(bind=engine)


# ---------------- Models ---------------- #

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String, unique=True)
    prabhupada_ratio = Column(Integer)
    answer_length = Column(String)
    answer_format = Column(String)
    devotee_level = Column(String)
    quiz_type = Column(String)
    difficulty = Column(String)
    num_questions = Column(Integer)

    queries = relationship("QueryHistory", back_populates="user")
    quizzes = relationship("QuizResult", back_populates="user")
    verses = relationship("VerseRead", back_populates="user")


class QueryHistory(Base):
    __tablename__ = "query_history"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    query = Column(Text)
    answer = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="queries")


class QuizResult(Base):
    __tablename__ = "quiz_results"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    quiz_type = Column(String)
    difficulty = Column(String)
    question = Column(Text)           # JSON string of question list
    correct_answer = Column(Text)     # JSON string of correct answers
    user_answer = Column(Text)        # JSON string of user answers
    total_correct = Column(Integer)
    references = Column(Text)         # JSON string of references
    timestamp = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="quizzes")


class VerseRead(Base):
    __tablename__ = "verses_read"
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    verse = Column(String)
    source = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="verses")


# ---------------- Helpers ---------------- #

def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_user_by_username(db, username: str):
    return db.query(User).filter(User.username == username).first()


def create_or_update_user(db, user_data: dict):
    user = get_user_by_username(db, user_data["username"])
    if user:
        for key, value in user_data.items():
            setattr(user, key, value)
    else:
        user = User(**user_data)
        db.add(user)
    db.commit()
    db.refresh(user)
    return user


def store_query(db, user_id: int, query: str, answer: dict):


    # Save verse if only one retrieval source
    sources = answer.get("retrieval_sources", [])
    if len(sources) == 1:
        context = answer.get("retrieved_context", "")
        verse_url = sources[0].get("url", "")
        if context and verse_url:
            store_verse_read(db, user_id, context, verse_url)
    else:
        record = QueryHistory(user_id=user_id, query=query, answer=answer["final_answer"])
        db.add(record)
        db.commit()



def store_quiz_result(db, user_id: int, quiz_type: str, difficulty: str,
                      questions: list, correct_answers: list, user_answers: list,
                      total_correct: int, references: list):
    record = QuizResult(
        user_id=user_id,
        quiz_type=quiz_type,
        difficulty=difficulty,
        question=json.dumps(questions, ensure_ascii=False),
        correct_answer=json.dumps(correct_answers, ensure_ascii=False),
        user_answer=json.dumps(user_answers, ensure_ascii=False),
        total_correct=total_correct,
        references=json.dumps(references, ensure_ascii=False)
    )
    db.add(record)
    db.commit()


def store_verse_read(db, user_id: int, verse: str, source: str):
    record = VerseRead(user_id=user_id, verse=verse, source=source)
    db.add(record)
    db.commit()
