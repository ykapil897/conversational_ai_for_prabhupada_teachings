# memory.py
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime

from .. import models, schemas, database

router = APIRouter(
    prefix="/memory",
    tags=["memory"],
    responses={404: {"description": "Not found"}}
)

@router.post("/", response_model=schemas.VerseMemory)
def create_verse_memory(
    memory: schemas.VerseMemoryCreate,
    db: Session = Depends(database.get_db)
):
    # Check if user exists
    user = db.query(models.User).filter(models.User.id == memory.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if memory already exists
    existing_memory = db.query(models.VerseMemory).filter(
        models.VerseMemory.user_id == memory.user_id,
        models.VerseMemory.verse_id == memory.verse_id
    ).first()
    
    if existing_memory:
        # Update existing memory
        existing_memory.bookmarked = memory.bookmarked
        existing_memory.notes = memory.notes
        existing_memory.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing_memory)
        return existing_memory
    
    # Create new memory
    db_memory = models.VerseMemory(**memory.dict())
    db.add(db_memory)
    db.commit()
    db.refresh(db_memory)
    return db_memory

@router.get("/user/{user_id}", response_model=List[schemas.VerseMemory])
def get_user_memories(user_id: int, db: Session = Depends(database.get_db)):
    memories = db.query(models.VerseMemory).filter(
        models.VerseMemory.user_id == user_id
    ).order_by(models.VerseMemory.updated_at.desc()).all()
    
    return memories

@router.get("/bookmarks/{user_id}", response_model=List[schemas.VerseMemory])
def get_user_bookmarks(user_id: int, db: Session = Depends(database.get_db)):
    bookmarks = db.query(models.VerseMemory).filter(
        models.VerseMemory.user_id == user_id,
        models.VerseMemory.bookmarked == True
    ).order_by(models.VerseMemory.updated_at.desc()).all()
    
    return bookmarks