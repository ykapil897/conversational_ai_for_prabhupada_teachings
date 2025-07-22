from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from db_manager import get_db, init_db, get_user_by_username, store_query, store_quiz_result, store_verse_read, create_or_update_user
from quiz_generator import QuizGenerator
from rag_testing_final import PrabhupadaRAG

app = FastAPI()
init_db()

# rag_model = PrabhupadaRAG()
# quiz_gen = QuizGenerator()

@app.get("/")
def hello():
    return {"message": "Hello, world!"}

class QueryRequest(BaseModel):
    username: str
    query: str


class QuizRequest(BaseModel):
    username: str
    topic: str
    quiz_type: str
    difficulty: str
    num_questions: int


class QuizSubmission(BaseModel):
    username: str
    quiz_data: dict
    user_answers: dict


class PreferenceUpdate(BaseModel):
    username: str
    prabhupada_ratio: int
    answer_length: str
    answer_format: str
    devotee_level: str
    quiz_type: str
    difficulty: str
    num_questions: int


@app.post("/process_query")
def process_query(data: QueryRequest, db: Session = Depends(get_db)):
    user = get_user_by_username(db, data.username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    preferences = {
        "prabhupada_ratio": int(user.prabhupada_ratio),
        "answer_length": user.answer_length,
        "answer_format": user.answer_format,
        "devotee_level": user.devotee_level
    }

    final_answer = rag_model.process_query(data.query, preferences["prabhupada_ratio"],
                                           preferences["answer_length"], preferences["answer_format"],
                                           preferences["devotee_level"])
    store_query(db, user.id, data.query, final_answer)
    return {"answer": final_answer}


@app.post("/generate_quiz")
def generate_quiz(data: QuizRequest, db: Session = Depends(get_db)):
    user = get_user_by_username(db, data.username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    quiz = quiz_gen.generate_quiz(
        query=data.topic,
        quiz_type=data.quiz_type,
        difficulty=data.difficulty,
        num_questions=data.num_questions
    )
    return quiz


@app.post("/submit_answers")
def submit_answers(submission: QuizSubmission, db: Session = Depends(get_db)):
    user = get_user_by_username(db, submission.username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    results = quiz_gen.check_answers(submission.quiz_data, submission.user_answers)

    questions = [q.get("statement") or q.get("question") for q in submission.quiz_data["questions"]]
    correct = [
        q["options"][q["answer"]] if submission.quiz_data["meta"]["quiz_type"] == "mcq" else q["answer"]
        for q in submission.quiz_data["questions"]
    ]
    user_ans = [submission.user_answers.get(str(i + 1)) for i in range(len(questions))]
    references = [q.get("reference") for q in submission.quiz_data["questions"]]

    store_quiz_result(
        db, user.id,
        quiz_type=submission.quiz_data["meta"]["quiz_type"],
        difficulty=submission.quiz_data["meta"]["difficulty"],
        questions=questions,
        correct_answers=correct,
        user_answers=user_ans,
        total_correct=results["correct_count"],
        references=references
    )

    return results


@app.post("/update_preferences")
def update_preferences(pref: PreferenceUpdate, db: Session = Depends(get_db)):
    updated_user = create_or_update_user(db, pref.dict())
    return {"status": "Preferences updated", "username": updated_user.username}


@app.get("/get_history/{username}")
def get_history(username: str, db: Session = Depends(get_db)):
    user = get_user_by_username(db, username)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    queries = [{"query": q.query, "answer": q.answer, "timestamp": q.timestamp} for q in user.queries]
    quizzes = [{
        "quiz_type": q.quiz_type,
        "difficulty": q.difficulty,
        "total_correct": q.total_correct,
        "questions": q.question,
        "user_answers": q.user_answer,
        "correct_answers": q.correct_answer,
        "references": q.references,
        "timestamp": q.timestamp
    } for q in user.quizzes]

    verses = [{"verse": v.verse, "source": v.source, "timestamp": v.timestamp} for v in user.verses]

    return {
        "queries": queries,
        "quizzes": quizzes,
        "verses": verses
    }
