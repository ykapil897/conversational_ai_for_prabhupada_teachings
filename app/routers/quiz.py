from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from .. import models, schemas, database
from ..services.quiz_generation import QuizService

router = APIRouter(
    prefix="/quiz",
    tags=["quiz"],
    responses={404: {"description": "Not found"}}
)

quiz_service = QuizService()

@router.post("/start", response_model=schemas.Quiz)
def start_quiz(
    request: schemas.QuizStartRequest,
    db: Session = Depends(database.get_db)
):
    """Create and start a new quiz."""
    try:
        return quiz_service.create_quiz(db, request)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create quiz: {str(e)}")

@router.post("/submit", response_model=schemas.Quiz)
def submit_quiz(
    submission: schemas.QuizSubmitRequest,
    db: Session = Depends(database.get_db)
):
    """Submit answers for a quiz and get the score."""
    try:
        return quiz_service.submit_answers(db, submission)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit quiz: {str(e)}")

@router.get("/history/{user_id}", response_model=List[schemas.Quiz])
def get_quiz_history(
    user_id: int,
    db: Session = Depends(database.get_db)
):
    """Get quiz history for a user."""
    quizzes = db.query(models.Quiz).filter(
        models.Quiz.user_id == user_id
    ).order_by(models.Quiz.created_at.desc()).all()
    
    return quizzes

@router.get("/{quiz_id}", response_model=schemas.Quiz)
def get_quiz(
    quiz_id: int,
    db: Session = Depends(database.get_db)
):
    """Get a specific quiz by ID."""
    quiz = db.query(models.Quiz).filter(models.Quiz.id == quiz_id).first()
    if not quiz:
        raise HTTPException(status_code=404, detail="Quiz not found")
    return quiz