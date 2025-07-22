import os
import re
import json
import time
from typing import List, Dict, Any
import unicodedata
from groq import Groq
from rag_testing_final import PrabhupadaRAG
import streamlit as st

api_key = st.secrets["groq"]["api_key"]

# Setup LLM client
client = Groq(api_key=api_key)

def normalize_text(text: str) -> str:
    """Remove diacritics and lower the text for fair comparison"""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join([c for c in nfkd if not unicodedata.combining(c)]).lower()

class QuizGenerator:
    def __init__(self):
        self.rag = PrabhupadaRAG()
        self.quiz_types = ["fill-in-blank", "true-false", "mcq", "matching"]
    
    def generate_quiz(self, query: str, quiz_type: str = None, difficulty: str = "medium", num_questions: int = 5) -> Dict[str, Any]:
        """
        Generate a quiz based on content from Srila Prabhupada's works.
        
        Args:
            query: User's query about the topic for the quiz
            quiz_type: Type of quiz (fill-in-blank, true-false, mcq, matching)
            difficulty: Difficulty level (easy, medium, hard)
            num_questions: Number of questions to generate
            
        Returns:
            Dictionary with quiz content
        """
        # Validate quiz type
        if quiz_type and quiz_type not in self.quiz_types:
            return {"error": f"Invalid quiz type. Choose from: {', '.join(self.quiz_types)}"}
        
        # If quiz type not specified, detect it from the query or default to mcq
        if not quiz_type:
            quiz_type = self._detect_quiz_type_from_query(query)
        
        # Validate num_questions
        num_questions = max(1, min(10, num_questions))  # Limit between 1 and 10
        
        print(f"🧠 Generating {quiz_type} quiz with {num_questions} {difficulty}-level questions")
        
        # Get content from RAG system
        rag_result = self.rag.get_context_for_quiz(query)
        
        if not rag_result["retrieved_context"]:
            return {"error": "Could not find relevant content for this quiz topic"}
        
        # Generate the quiz
        quiz_content = self._create_quiz_from_context(
            rag_result["retrieved_context"],
            rag_result["retrieval_sources"],
            quiz_type,
            difficulty,
            num_questions
        )
        
        # Add metadata
        quiz_content["meta"] = {
            "query": query,
            "refined_query": rag_result["refined_query"],
            "quiz_type": quiz_type,
            "difficulty": difficulty,
            "num_questions": num_questions,
            "search_time": rag_result["search_time"]
        }
        
        return quiz_content
    
    def _detect_quiz_type_from_query(self, query: str) -> str:
        """Detect the quiz type from the query"""
        query_lower = query.lower()
        
        if any(term in query_lower for term in ["fill", "blank", "fill in", "fill-in", "fill in the blank"]):
            return "fill-in-blank"
        elif any(term in query_lower for term in ["true", "false", "true or false", "true/false"]):
            return "true-false"
        elif any(term in query_lower for term in ["mcq", "multiple choice", "options"]):
            return "mcq"
        elif any(term in query_lower for term in ["match", "matching", "match the following"]):
            return "matching"
        else:
            # Default to MCQ if no specific type detected
            return "mcq"
    
    def _create_quiz_from_context(
        self, 
        context: str, 
        sources: List[Dict], 
        quiz_type: str, 
        difficulty: str, 
        num_questions: int
    ) -> Dict[str, Any]:
        """Create a quiz based on the retrieved context"""
        quiz_generation_start = time.time()
        
        # Get prompt based on quiz type
        prompt = self._get_quiz_generation_prompt(quiz_type, difficulty, num_questions)
        
        # Generate the quiz using LLM
        quiz_response = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": prompt
                },
                {
                    "role": "user",
                    "content": f"Context from Srila Prabhupada's works:\n\n{context}\n\nGenerate a {quiz_type} quiz with {num_questions} questions at {difficulty} difficulty level."
                },
            ],
            model="llama3-70b-8192",
            temperature=0.4,
            max_tokens=2000
        )
        
        quiz_text = quiz_response.choices[0].message.content
        
        # Process the quiz text into structured format
        quiz_data = self._process_quiz_text(quiz_text, quiz_type, sources)
        
        quiz_generation_time = time.time() - quiz_generation_start
        print(f"⏱️ Quiz generation completed in {quiz_generation_time:.2f} seconds")
        
        quiz_data["generation_time"] = quiz_generation_time
        
        return quiz_data
    
    def _get_quiz_generation_prompt(self, quiz_type: str, difficulty: str, num_questions: int) -> str:
        """Get the appropriate prompt for quiz generation based on type"""
        base_prompt = (
            "You are an expert on Srila Prabhupada's teachings who creates educational quizzes. "
            "Create a quiz based solely on the provided context. "
            f"Generate exactly {num_questions} questions at {difficulty} difficulty level.\n\n"
            "IMPORTANT GUIDELINES:\n"
            "1. Only use information explicitly stated in the context\n"
            "2. Include exact references (book, chapter, verse) for each question\n"
            "3. Make questions clear, unambiguous, and doctrinally accurate\n"
            "4. Format your response in a structured JSON-compatible format\n"
            "5. For every question, include the exact quote or source information and URL as the 'reference'\n\n"
        )
        
        if quiz_type == "fill-in-blank":
            base_prompt += (
                "FORMAT - FILL IN THE BLANK:\n"
                "If a **translation** is available, generate fill-in-the-blank **only from the translation**, not the purport or quote.\n"
                "Otherwise, use the main Prabhupada quote or purport.\n\n"
                
                "Create only **very short blanks (2–4 words max)** focusing on the **most important spiritual terms or phrases** (e.g., Krishna, hearing, devotional service, detachment, etc.).\n\n"
                
                "Avoid long blanks or trivial missing words. Maintain clarity and integrity of meaning.\n\n"
                
                "FORMAT EACH QUESTION AS:\n"
                "```json\n"
                "{\n"
                "  \"questions\": [\n"
                "    {\n"
                "      \"question\": \"According to Bhagavad-gita, real knowledge begins with _____.\",\n"
                "      \"answer\": \"detachment\",\n"
                "      \"reference\": \"SOURCE: Bg. 2.12 (translation) | QUOTE: 'Such a person is not bewildered and begins with detachment.'\",\n"
                "      \"url\": \"https://example.com/bg/2/12\"\n"
                "    }\n"
                "  ]\n" # for fill in the blanks translation us taken as only source .
                "}\n"
                "```\n"
            )

        elif quiz_type == "true-false":
            base_prompt += (
                "FORMAT - TRUE/FALSE:\n"
                "For each question:\n"
                "- Create a clear statement based on the context\n"
                "- Mark it as true or false\n"
                "- Include exact reference and URL\n"
                "- Provide a brief explanation why it's true or false\n\n"
                
                "FORMAT YOUR RESPONSE AS:\n"
                "```json\n"
                "{\n"
                "  \"questions\": [\n"
                "    {\n"
                "      \"statement\": \"According to Srila Prabhupada, chanting the holy name is the recommended process for self-realization in this age.\",\n"
                "      \"answer\": \"True\",\n"
                "      \"explanation\": \"This is explicitly stated in the text as the recommended process for Kali-yuga.\",\n"
                "      \"reference\": \"SOURCE: SB 1.1.1 (purport) | QUOTE: 'The chanting of the holy name is the recommended process for self-realization in this age'\",\n"
                "      \"url\": \"https://example.com/sb/1/1/1\"\n"
                "    }\n"
                "  ]\n"
                "}\n"
                "```\n"
                
                "NOTE: Create a balanced mix of true and false statements.\n"
            )
        
        elif quiz_type == "mcq":
            base_prompt += (
                "FORMAT - MULTIPLE CHOICE:\n"
                "For each question:\n"
                "- Create a clear question\n"
                "- Provide 4 options (labeled A, B, C, D)\n"
                "- Mark the correct answer\n"
                "- Include exact reference and URL\n\n"
                
                "FORMAT YOUR RESPONSE AS:\n"
                "```json\n"
                "{\n"
                "  \"questions\": [\n"
                "    {\n"
                "      \"question\": \"According to Srila Prabhupada, what is the goal of human life?\",\n"
                "      \"options\": {\n"
                "        \"A\": \"Material prosperity\",\n"
                "        \"B\": \"Self-realization and love of God\",\n"
                "        \"C\": \"Advanced technological development\",\n"
                "        \"D\": \"Physical comfort and mental peace\"\n"
                "      },\n"
                "      \"answer\": \"B\",\n"
                "      \"reference\": \"SOURCE: TYS Chapter 1 | QUOTE: 'The goal of human life is to realize God and develop love for Him'\",\n"
                "      \"url\": \"https://example.com/tys/1\"\n"
                "    }\n"
                "  ]\n"
                "}\n"
                "```\n"
                
                "NOTE: Make sure incorrect options are plausible but clearly wrong based on the context.\n"
            )
        
        elif quiz_type == "matching":
            base_prompt += (
                "FORMAT - MATCHING:\n"
                "Create two columns with terms/concepts in column A and their definitions/explanations in column B\n"
                "- Create {num_questions} pairs to match\n"
                "- Randomize the order of column B\n"
                "- Include exact references and URLs for each pair\n\n"
                
                "FORMAT YOUR RESPONSE AS:\n"
                "```json\n"
                "{\n"
                "  \"columnA\": [\n"
                "    \"Bhakti\",\n"
                "    \"Maya\",\n"
                "    \"Karma\"\n"
                "  ],\n"
                "  \"columnB\": [\n"
                "    \"Illusion or energy that binds one to material existence\",\n"
                "    \"Devotional service to the Supreme Lord\",\n"
                "    \"The law of action and reaction\"\n"
                "  ],\n"
                "  \"answers\": {\n"
                "    \"Bhakti\": \"Devotional service to the Supreme Lord\",\n"
                "    \"Maya\": \"Illusion or energy that binds one to material existence\",\n"
                "    \"Karma\": \"The law of action and reaction\"\n"
                "  },\n"
                "  \"references\": {\n"
                "    \"Bhakti\": {\n"
                "      \"reference\": \"SOURCE: NOD Chapter 1 | QUOTE: 'Bhakti means devotional service to the Supreme Lord'\",\n"
                "      \"url\": \"https://example.com/nod/1\"\n"
                "    },\n"
                "    \"Maya\": {\n"
                "      \"reference\": \"SOURCE: Bg. 7.14 | QUOTE: 'Maya, the illusory energy that binds the living entity to material existence'\",\n"
                "      \"url\": \"https://example.com/bg/7/14\"\n"
                "    }\n"
                "  }\n"
                "}\n"
                "```\n"
            )
        
        # Add difficulty-specific instructions
        if difficulty == "easy":
            base_prompt += (
                "DIFFICULTY - EASY:\n"
                "- Use direct quotes and basic concepts\n"
                "- Focus on explicit information from the text\n"
                "- Keep language simple and straightforward\n"
            )
        elif difficulty == "medium":
            base_prompt += (
                "DIFFICULTY - MEDIUM:\n"
                "- Include both direct quotes and implied concepts\n"
                "- Test understanding rather than just recall\n"
                "- Use moderate terminology from Prabhupada's teachings\n"
            )
        elif difficulty == "hard":
            base_prompt += (
                "DIFFICULTY - HARD:\n"
                "- Include nuanced philosophical points\n"
                "- Test deeper understanding and connections between concepts\n"
                "- Use proper Sanskrit terminology where appropriate\n"
                "- Create questions that require synthesis of multiple parts of the text\n"
            )

            base_prompt += (
                "6. Identify the most spiritually essential ideas or teachings—avoid trivial or context-less details.\n"
                "7. Prefer teachings that are repeated or emphasized by Srila Prabhupada.\n"
                "8. Avoid asking questions based solely on names or events unless they have clear philosophical weight.\n"
            )
        
        return base_prompt
    
    def _process_quiz_text(self, quiz_text: str, quiz_type: str, sources: List[Dict]) -> Dict[str, Any]:
        """Process the quiz text into a structured format"""
        # Try to extract JSON data
        json_match = re.search(r'```json\s*([\s\S]+?)\s*```', quiz_text)
        
        if json_match:
            try:
                quiz_data = json.loads(json_match.group(1))
                # Validate basic structure
                if quiz_type == "matching" and "columnA" in quiz_data and "columnB" in quiz_data and "answers" in quiz_data:
                    pass  # Valid matching quiz
                elif "questions" in quiz_data and isinstance(quiz_data["questions"], list):
                    pass  # Valid question-based quiz
                else:
                    raise ValueError("Invalid quiz data structure")
                
                # Add source materials for reference
                quiz_data["sources"] = [
                    {
                        "verse_id": source.get("verse_id", ""),
                        "book": source.get("book", ""),
                        "specific_book": source.get("specific_book", ""),
                        "url": source.get("url", "")
                    }
                    for source in sources
                ]
                
                return quiz_data
            
            except json.JSONDecodeError:
                # If JSON parsing fails, return the raw text
                pass
        
        # If we couldn't extract valid JSON, try to parse it manually
        questions = []
        sections = re.split(r'\n\s*\d+[\.\)]\s+', '\n' + quiz_text)
        
        if len(sections) > 1:
            for i, section in enumerate(sections[1:], 1):  # Skip the first empty split
                questions.append({
                    "question_number": i,
                    "text": section.strip()
                })
        
        return {
            "raw_quiz": quiz_text,
            "parsed_questions": questions if questions else [],
            "sources": [
                {
                    "verse_id": source.get("verse_id", ""),
                    "book": source.get("book", ""),
                    "specific_book": source.get("specific_book", ""),
                    "url": source.get("url", "")
                }
                for source in sources
            ]
        }
    
    def check_answers(self, quiz_data: Dict[str, Any], user_answers: Dict[str, Any]) -> Dict[str, Any]:
        results = {
            "correct_count": 0,
            "total_questions": 0,
            "questions": []
        }

        quiz_type = quiz_data.get("meta", {}).get("quiz_type", "")

        if quiz_data.get("questions"):
            for i, question in enumerate(quiz_data["questions"]):
                q_id = str(i + 1)
                user_answer = user_answers.get(q_id)
                correct_answer = question.get("answer")

                # For MCQs: convert correct_answer from key to value
                if quiz_type == "mcq" and isinstance(correct_answer, str) and "options" in question:
                    options = question["options"]
                    correct_answer = options.get(correct_answer)

                # Case-insensitive comparison
                is_correct = False
                if user_answer is not None and correct_answer is not None:
                    is_correct = normalize_text(str(user_answer).strip()) == normalize_text(str(correct_answer).strip())

                if is_correct:
                    results["correct_count"] += 1

                results["questions"].append({
                    "question_id": q_id,
                    "user_answer": user_answer,
                    "correct_answer": correct_answer,
                    "is_correct": is_correct
                })

            results["total_questions"] = len(quiz_data["questions"])
            results["score_percentage"] = (
                (results["correct_count"] / results["total_questions"]) * 100
                if results["total_questions"] > 0 else 0
            )

        elif quiz_type == "matching" and quiz_data.get("columnA") and quiz_data.get("answers"):
            for item in quiz_data["columnA"]:
                q_id = item
                user_answer = user_answers.get(q_id)
                correct_answer = quiz_data["answers"].get(q_id)

                is_correct = False
                if user_answer and correct_answer:
                    is_correct = str(user_answer).strip().lower() == str(correct_answer).strip().lower()

                if is_correct:
                    results["correct_count"] += 1

                results["questions"].append({
                    "question_id": q_id,
                    "user_answer": user_answer,
                    "correct_answer": correct_answer,
                    "is_correct": is_correct
                })

            results["total_questions"] = len(quiz_data["columnA"])
            results["score_percentage"] = (
                (results["correct_count"] / results["total_questions"]) * 100
                if results["total_questions"] > 0 else 0
            )

        return results


# Sample use of the quiz generator
if __name__ == "__main__":
    quiz_gen = QuizGenerator()
    
    # Example quiz generation
    quiz_query = "provide answer from chapter 5 in canto 4 in sb"
    quiz_type = "fill-in-blank" # fill-in-blank, true-false, mcq, matching
    difficulty = "hard"  # easy, medium, hard
    num_questions = "10"
    
    # Set default values if not provided
    if not quiz_type.strip():
        quiz_type = None  # Auto-detect
    if not difficulty.strip():
        difficulty = "medium"
    if not num_questions.strip() or not num_questions.isdigit():
        num_questions = 5
    else:
        num_questions = int(num_questions)
    
    # Generate quiz
    quiz = quiz_gen.generate_quiz(
        query=quiz_query,
        quiz_type=quiz_type,
        difficulty=difficulty,
        num_questions=num_questions
    )
    
    # Save quiz to file
    if "error" not in quiz:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        with open(f"quiz/quiz_{timestamp}.json", "w") as f:
            json.dump(quiz, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Quiz saved to quiz_{timestamp}.json")
        
        # Print quiz questions
        if "questions" in quiz:
            print("\n📝 QUIZ QUESTIONS:\n")
            for i, q in enumerate(quiz["questions"], 1):
                if "question" in q:
                    print(f"{i}. {q['question']}")
                    if "options" in q:
                        for opt, text in q["options"].items():
                            print(f"   {opt}) {text}")
                    print()
                elif "statement" in q:
                    print(f"{i}. True or False: {q['statement']}")
                    print()
        elif "columnA" in quiz and "columnB" in quiz:
            print("\n📝 MATCHING QUIZ:\n")
            print("Column A:")
            for i, item in enumerate(quiz["columnA"], 1):
                print(f"{i}. {item}")
            print("\nColumn B:")
            for i, item in enumerate(quiz["columnB"], 1):
                print(f"{chr(96+i)}. {item}")
            print()
    else:
        print(f"\n⚠️ Error: {quiz['error']}")