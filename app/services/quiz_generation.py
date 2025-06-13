import json
import random
import re
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from .. import models, schemas
from .rag_service import RAGService

class QuizService:
    def __init__(self):
        self.rag_service = RAGService()
        
    def create_quiz(self, 
                    db: Session, 
                    quiz_request: schemas.QuizStartRequest) -> models.Quiz:
        """Create a new quiz and generate questions based on the request."""
        
        # 1. Create quiz in database
        db_quiz = models.Quiz(
            user_id=quiz_request.user_id,
            quiz_type=quiz_request.quiz_type,
            source_type=quiz_request.source_type,
            chapter=quiz_request.chapter,
            verse_range=quiz_request.verse_range,
            max_score=quiz_request.num_questions
        )
        
        db.add(db_quiz)
        db.commit()
        db.refresh(db_quiz)
        
        # 2. Generate questions and add to database
        try:
            questions = self._generate_questions(db_quiz, quiz_request)
            
            for question in questions:
                db_question = models.QuizQuestion(
                    quiz_id=db_quiz.id,
                    question_text=question["question_text"],
                    question_type=quiz_request.quiz_type,
                    verse_id=question["verse_id"],
                    options=json.dumps(question["options"]),
                    correct_answer=question["correct_answer"]
                )
                db.add(db_question)
            
            db.commit()
            db.refresh(db_quiz)
            return db_quiz
            
        except Exception as e:
            # Clean up if generation fails
            db.query(models.Quiz).filter(models.Quiz.id == db_quiz.id).delete()
            db.commit()
            raise e
    
    def _generate_questions(self, 
                           quiz: models.Quiz, 
                           quiz_request: schemas.QuizStartRequest) -> List[Dict[str, Any]]:
        """Generate quiz questions using LLM."""
        
        # 1. Get content based on chapter/verse selection
        content = self._retrieve_content_for_quiz(quiz_request)
        
        # 2. Generate quiz questions using LLM
        if quiz_request.quiz_type == "mcq":
            if quiz_request.source_type == "purport":
                prompt = self._get_mcq_from_purport_prompt(content, quiz_request.num_questions)
            else:
                prompt = self._get_mcq_from_verse_prompt(content, quiz_request.num_questions)
        else:  # cloze
            prompt = self._get_cloze_from_verse_prompt(content, quiz_request.num_questions)
        
        # 3. Use RAG service to process the prompt
        response = self.rag_service.process_prompt_directly(prompt)
        
        # 4. Parse the response into structured questions
        questions = self._parse_questions(response, quiz_request.quiz_type)
        return questions
    
    def _retrieve_content_for_quiz(self, quiz_request: schemas.QuizStartRequest) -> str:
        """Retrieve content for the quiz based on chapter and verse range."""
        # Implementation depends on how you store the Gita content
        # This is a simplified version
        
        # Get chapter content
        chapter_query = f"Chapter {quiz_request.chapter}"
        if quiz_request.chapter:
            result = self.rag_service.retrieve_chapter_content(quiz_request.chapter)
            content = result["content"]
            
            # Filter by verse range if specified
            if quiz_request.verse_range and quiz_request.verse_range != "all":
                try:
                    if "-" in quiz_request.verse_range:
                        start, end = map(int, quiz_request.verse_range.split("-"))
                        content = self._filter_verses(content, quiz_request.chapter, start, end)
                    else:
                        verse = int(quiz_request.verse_range)
                        content = self._filter_verses(content, quiz_request.chapter, verse, verse)
                except:
                    pass  # If parsing fails, use entire chapter
                
            # Filter by source type (purport, verse, or translation)
            if quiz_request.source_type == "purport":
                content = self._extract_purports(content)
            elif quiz_request.source_type == "verse":
                content = self._extract_verses(content)
            elif quiz_request.source_type == "translation":
                content = self._extract_translations(content)
                
            return content
        else:
            # If no chapter specified, get some relevant content from the Gita
            return self._get_sample_content()
    
    def _filter_verses(self, content: str, chapter: int, start: int, end: int) -> str:
        """Filter content to keep only verses in the specified range."""
        lines = content.split('\n')
        result = []
        
        current_verse = None
        include_current = False
        
        for line in lines:
            # Check if this is a verse reference line
            verse_match = re.search(r'REFERENCE: Bg\.\s*(\d+)\.(\d+)', line)
            if verse_match:
                chapter_num = int(verse_match.group(1))
                verse_num = int(verse_match.group(2))
                current_verse = verse_num
                include_current = (chapter_num == chapter and start <= verse_num <= end)
            
            # Include the line if it's part of the current verse and within range
            if include_current or 'REFERENCE:' in line:
                result.append(line)
                
        return '\n'.join(result)
    
    def _extract_purports(self, content: str) -> str:
        """Extract only purport sections from the content."""
        sections = re.split(r'PURPORT BY SRILA PRABHUPADA:', content)
        purports = []
        
        # Skip first section (it's before the first purport)
        for i in range(1, len(sections)):
            section = sections[i]
            # Get the verse reference from the previous section
            match = re.search(r'REFERENCE: (Bg\.\s*\d+\.\d+)', sections[i-1])
            if match:
                verse_ref = match.group(1)
                # Add the verse reference and the purport
                purport_text = section.split('URL:')[0].strip()
                purports.append(f"REFERENCE: {verse_ref}\nPURPORT BY SRILA PRABHUPADA: {purport_text}")
        
        return '\n\n'.join(purports)
    
    def _extract_verses(self, content: str) -> str:
        """Extract only verse text sections from the content."""
        sections = re.split(r'VERSE:', content)
        verses = []
        
        # Skip first section (it's before the first verse)
        for i in range(1, len(sections)):
            section = sections[i]
            # Get the verse reference from the previous section or this section
            match = re.search(r'REFERENCE: (Bg\.\s*\d+\.\d+)', sections[i-1])
            if not match:
                match = re.search(r'REFERENCE: (Bg\.\s*\d+\.\d+)', section)
            
            if match:
                verse_ref = match.group(1)
                # Add the verse reference and the verse text
                verse_text = section.split('TRANSLATION:')[0].strip()
                verses.append(f"REFERENCE: {verse_ref}\nVERSE: {verse_text}")
        
        return '\n\n'.join(verses)
    
    def _extract_translations(self, content: str) -> str:
        """Extract only translation sections from the content."""
        sections = re.split(r'TRANSLATION:', content)
        translations = []
        
        # Skip first section (it's before the first translation)
        for i in range(1, len(sections)):
            section = sections[i]
            # Get the verse reference from the previous section
            match = re.search(r'REFERENCE: (Bg\.\s*\d+\.\d+)', sections[i-1])
            if match:
                verse_ref = match.group(1)
                # Add the verse reference and the translation
                translation_text = section.split('PURPORT BY SRILA PRABHUPADA:')[0].split('URL:')[0].strip()
                translations.append(f"REFERENCE: {verse_ref}\nTRANSLATION: {translation_text}")
        
        return '\n\n'.join(translations)
    
    def _get_sample_content(self) -> str:
        """Get some sample content if no specific chapter is chosen."""
        # This could randomly select a few verses or a chapter section
        # For simplicity, we'll use a static example from chapter 2
        chapter_query = "Chapter 2"
        result = self.rag_service.retrieve_chapter_content(2)
        return result["content"]
    
    def _get_mcq_from_purport_prompt(self, content: str, num_questions: int) -> str:
        """Create prompt for generating MCQ questions from purports."""
        return f"""Generate {num_questions} multiple-choice questions based on the purports of Bhagavad Gita verses below. 
For each question:
1. Create a clear question based on the purport's content
2. Provide 4 options (A, B, C, D)
3. Indicate the correct answer
4. Include the verse reference (e.g., Bg. 2.47)

Format each question as:
Q1. [Question text]
A) [Option A]
B) [Option B]
C) [Option C]
D) [Option D]
Correct Answer: [Letter]
Verse Reference: [Verse ID]

Here are the purports to use:

{content}

Generate exactly {num_questions} multiple-choice questions that test understanding of key concepts from these purports.
"""

    def _get_mcq_from_verse_prompt(self, content: str, num_questions: int) -> str:
        """Create prompt for generating MCQ questions from verses/translations."""
        return f"""Generate {num_questions} multiple-choice questions based on the verses and translations of Bhagavad Gita below. 
For each question:
1. Create a clear question based on the verse or translation content
2. Provide 4 options (A, B, C, D)
3. Indicate the correct answer
4. Include the verse reference (e.g., Bg. 2.47)

Format each question as:
Q1. [Question text]
A) [Option A]
B) [Option B]
C) [Option C]
D) [Option D]
Correct Answer: [Letter]
Verse Reference: [Verse ID]

Here are the verses/translations to use:

{content}

Generate exactly {num_questions} multiple-choice questions that test understanding of key teachings from these verses.
"""

    def _get_cloze_from_verse_prompt(self, content: str, num_questions: int) -> str:
        """Create prompt for generating cloze-style questions from verses/translations."""
        return f"""Generate {num_questions} cloze (fill-in-the-blank) questions based on the verses and translations of Bhagavad Gita below.
For each question:
1. Select an important verse or translation
2. Create a cloze question by replacing 1-3 key words with blanks (_______)
3. Provide the correct answer(s) that should fill in the blank(s)
4. Include the verse reference (e.g., Bg. 2.47)

Format each question as:
Q1. [Sentence with _____ blanks]
Correct Answer(s): [Word or words to fill in the blanks, separated by commas if multiple]
Verse Reference: [Verse ID]

Here are the verses/translations to use:

{content}

Generate exactly {num_questions} cloze questions that test understanding of important concepts and teachings.
"""

    def _parse_questions(self, response: str, quiz_type: str) -> List[Dict[str, Any]]:
        """Parse the LLM response into structured questions."""
        questions = []
        
        # Split response into individual questions
        question_blocks = re.split(r'Q\d+\.', response)[1:]  # Skip first split (before Q1)
        
        for block in question_blocks:
            try:
                if quiz_type == "mcq":
                    # Parse MCQ format
                    question_text = block.split('A)')[0].strip()
                    options_text = 'A)' + block.split('A)')[1].split('Correct Answer:')[0]
                    correct_answer = re.search(r'Correct Answer:\s*([A-D])', block).group(1)
                    verse_id = re.search(r'Verse Reference:\s*(Bg\.\s*\d+\.\d+)', block).group(1)
                    
                    # Extract options
                    options = {}
                    for opt in ['A', 'B', 'C', 'D']:
                        match = re.search(fr'{opt}\)\s*([^\n]+?)(?=[A-D]\)|Correct Answer:|$)', options_text)
                        if match:
                            options[opt] = match.group(1).strip()
                    
                    questions.append({
                        "question_text": question_text,
                        "options": options,
                        "correct_answer": correct_answer,
                        "verse_id": verse_id
                    })
                    
                else:  # cloze
                    # Parse cloze format
                    question_text = block.split('Correct Answer')[0].strip()
                    correct_answer = re.search(r'Correct Answer\(?s?\)?:\s*([^\n]+)', block).group(1).strip()
                    verse_id = re.search(r'Verse Reference:\s*(Bg\.\s*\d+\.\d+)', block).group(1)
                    
                    # For cloze, options are the correct answers
                    options = {
                        "answers": [ans.strip() for ans in correct_answer.split(',')]
                    }
                    
                    questions.append({
                        "question_text": question_text,
                        "options": options,
                        "correct_answer": correct_answer,
                        "verse_id": verse_id
                    })
            except Exception as e:
                # Skip malformed questions
                print(f"Error parsing question: {e}")
                continue
                
        return questions
    
    def submit_answers(self, db: Session, submission: schemas.QuizSubmitRequest) -> models.Quiz:
        """Process quiz submission and calculate score."""
        quiz = db.query(models.Quiz).filter(models.Quiz.id == submission.quiz_id).first()
        
        if not quiz:
            raise ValueError(f"Quiz with ID {submission.quiz_id} not found")
        
        # Process each answer
        correct_count = 0
        max_score = 0
        
        for answer in submission.answers:
            question = db.query(models.QuizQuestion).filter(
                models.QuizQuestion.id == answer.question_id,
                models.QuizQuestion.quiz_id == quiz.id
            ).first()
            
            if not question:
                continue
                
            max_score += 1
            
            # Update the question with user's answer
            question.user_answer = answer.user_answer
            
            # Check if answer is correct
            if quiz.quiz_type == "mcq":
                question.is_correct = (question.correct_answer.strip() == answer.user_answer.strip())
            else:  # cloze
                # For cloze questions, check if the answer matches any of the acceptable answers
                correct_answers = json.loads(question.options).get("answers", [])
                question.is_correct = any(
                    answer.user_answer.lower().strip() == correct.lower().strip() 
                    for correct in correct_answers
                )
            
            if question.is_correct:
                correct_count += 1
        
        # Update quiz score and completion
        quiz.score = correct_count
        quiz.max_score = max_score
        quiz.completed_at = func.now()
        
        db.commit()
        db.refresh(quiz)
        return quiz