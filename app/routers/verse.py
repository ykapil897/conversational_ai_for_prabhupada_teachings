# verse.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
import re

from .. import models, schemas, database

router = APIRouter(
    prefix="/verse",
    tags=["verse"],
    responses={404: {"description": "Not found"}}
)

@router.post("/history", response_model=schemas.VerseHistory)
def add_verse_history(
    verse: schemas.VerseHistoryCreate,
    db: Session = Depends(database.get_db)
):
    # Validate verse_id format (e.g., "Bg. 2.47")
    if not re.match(r"Bg\.\s*\d+\.\d+", verse.verse_id):
        raise HTTPException(status_code=400, detail="Invalid verse ID format, should be like 'Bg. 2.47'")
    
    # Check if session exists
    db_session = db.query(models.Session).filter(models.Session.id == verse.session_id).first()
    if db_session is None:
        raise HTTPException(status_code=404, detail="Session not found")
    
    # Create verse history entry
    db_verse_history = models.VerseHistory(**verse.dict())
    db.add(db_verse_history)
    db.commit()
    db.refresh(db_verse_history)
    return db_verse_history

@router.get("/history/{session_id}", response_model=List[schemas.VerseHistory])
def get_verse_history(session_id: int, db: Session = Depends(database.get_db)):
    histories = db.query(models.VerseHistory).filter(
        models.VerseHistory.session_id == session_id
    ).order_by(models.VerseHistory.timestamp.desc()).all()
    return histories

@router.get("/last/{session_id}", response_model=schemas.VerseHistory)
def get_last_verse(session_id: int, db: Session = Depends(database.get_db)):
    verse = db.query(models.VerseHistory).filter(
        models.VerseHistory.session_id == session_id
    ).order_by(models.VerseHistory.timestamp.desc()).first()
    
    if verse is None:
        raise HTTPException(status_code=404, detail="No verse history found for this session")
    
    return verse