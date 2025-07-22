# session.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta
import uuid

from .. import models, schemas, database

router = APIRouter(
    prefix="/session",
    tags=["session"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=schemas.Session)
def create_session(user_id: int, db: Session = Depends(database.get_db)):
    # Check if user exists
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Create new session
    session_token = str(uuid.uuid4())
    db_session = models.Session(user_id=user_id, session_token=session_token)
    db.add(db_session)
    db.commit()
    db.refresh(db_session)
    return db_session

@router.get("/{session_token}", response_model=schemas.Session)
def get_session(session_token: str, db: Session = Depends(database.get_db)):
    db_session = db.query(models.Session).filter(models.Session.session_token == session_token).first()
    if db_session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Update streak if needed
    last_active = db_session.last_active_at
    now = datetime.utcnow()
    
    # If last activity was yesterday, increase streak
    if (now.date() - last_active.date()) == timedelta(days=1):
        db_session.streak_days += 1
        db.commit()
        db.refresh(db_session)
    # If more than one day has passed, reset streak to 1
    elif (now.date() - last_active.date()) > timedelta(days=1):
        db_session.streak_days = 1
        db.commit()
        db.refresh(db_session)
        
    # Update last active time
    db_session.last_active_at = now
    db.commit()
    db.refresh(db_session)
    
    return db_session

@router.put("/{session_token}/query", response_model=schemas.Session)
def update_last_query(session_token: str, query: str, db: Session = Depends(database.get_db)):
    db_session = db.query(models.Session).filter(models.Session.session_token == session_token).first()
    if db_session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    
    db_session.last_query = query
    db_session.last_active_at = datetime.utcnow()
    db.commit()
    db.refresh(db_session)
    return db_session

@router.put("/{session_token}/devotee_level", response_model=schemas.Session)
def update_devotee_level(
    session_token: str, 
    level: str = "intermediate",
    db: Session = Depends(database.get_db)
):
    """Update the devotee level of the user."""
    # Validate level
    if level not in ["neophyte", "intermediate", "advanced"]:
        raise HTTPException(status_code=400, detail="Invalid devotee level. Must be 'neophyte', 'intermediate' or 'advanced'")
        
    db_session = db.query(models.Session).filter(models.Session.session_token == session_token).first()
    if db_session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Get or create user preferences
    user_prefs = db.query(models.UserPreference).filter(
        models.UserPreference.user_id == db_session.user_id
    ).first()
    
    if not user_prefs:
        user_prefs = models.UserPreference(user_id=db_session.user_id)
        db.add(user_prefs)
    
    # Update devotee level
    user_prefs.devotee_level = level
    db.commit()
    db.refresh(db_session)
    
    return db_session