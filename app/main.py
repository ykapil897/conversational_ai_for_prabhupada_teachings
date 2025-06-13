# main.py
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import os
import json

from . import models, schemas, database
from .database import engine
from .routers import session, verse, memory, quiz
from .services.rag_service import RAGService

# Create tables in the database
models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Bhagavad Gita RAG API", 
              description="API for Bhagavad Gita RAG with session tracking",
              version="1.0.0")

# Include routers
app.include_router(session.router)
app.include_router(verse.router)
app.include_router(memory.router)
app.include_router(quiz.router) 

# Initialize RAG service
rag_service = RAGService()

@app.get("/")
def read_root():
    return {"message": "Welcome to Bhagavad Gita RAG API"}

@app.post("/query", response_model=schemas.QueryResponse)
def process_query(
    request: schemas.QueryRequest,
    db: Session = Depends(database.get_db)
):
    try:
        result = rag_service.process_query_with_session(
            query=request.query,
            session_token=request.session_token,
            db=db
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/users/", response_model=schemas.User)
def create_user(user: schemas.UserCreate, db: Session = Depends(database.get_db)):
    # Check if user with email already exists
    db_user = db.query(models.User).filter(models.User.email == user.email).first()
    if db_user:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    # Create new user
    db_user = models.User(username=user.username, email=user.email)
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user