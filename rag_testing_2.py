import os
import faiss
import json
import re
import numpy as np
from sentence_transformers import SentenceTransformer
from groq import Groq

# 1️⃣ Load embedding model
model = SentenceTransformer("BAAI/bge-large-en")

# 2️⃣ Load FAISS index
index = faiss.read_index("gita_index.faiss")

# 3️⃣ Load metadata (structured and formatted)
with open("gita_metadata.json", "r") as f:
    structured_metadata = json.load(f)

formatted_metadata = [
    f"{entry['verse_id']} — {entry['verse_text']} — {entry['translation']} — {entry['purport']}"
    for entry in structured_metadata
]

# 4️⃣ Setup Groq
client = Groq(api_key="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")

# 🔍 Query input
original_query = "Explain what is Liberation from previously discussed chapter"
history_path = "history.json"
try:
    with open(history_path, "r") as f:
        history = json.load(f)
        previous_queries = history[-3:]
except (FileNotFoundError, json.JSONDecodeError):
    previous_queries = []

# 5️⃣ STAGE 1: Query Refinement with LLM
print(f"🔍 Original query: '{original_query}'")
history = previous_queries + [original_query]
with open(history_path, "w") as f:
    json.dump(history, f, indent=2)
previous_context_block = "\n".join(
    [f"{i+1}. {q}" for i, q in enumerate(previous_queries)]
) if previous_queries else "None"
query_refinement_response = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": (
                "You are a query refinement expert for a Bhagavad Gita retrieval system. "
                "Your job is to convert user queries into effective search queries that will match relevant verses. "
                "Maintain the original intent and key concepts from the user's query. "
                "If the original query mentions specific chapters or verses (e.g., 'Chapter 2', 'Bg 3.4'), preserve these references. "
                "Make the refined query concise and focused on the key concepts that would appear in the Bhagavad Gita text. "
                "If the query is already optimal, you may keep it as is."  
                "Use the previous queries if relevant to refine the current query. "
                "Otherwise, just focus on the current query. Be accurate and brief."
                "Use only the last 1–3 previous queries if they are **clearly relevant** to the current query else consider the previous query\n"

            )
        },
        {
            "role": "user",
            "content": (
                f"Here are the user's 3 previous queries:\n{previous_context_block}\n\n"
                f"Current query:\n{original_query}\n\n"
              
                "Return only the refined version of the current query:"
   )
        },
    ],
    model="llama3-70b-8192",
    temperature=0.2,
    top_p=0.2,
    max_tokens=100
)

refined_query = query_refinement_response.choices[0].message.content.strip()
refined_query = re.sub(r'^"(.+)"$', r'\1', refined_query)
print(f"🔄 Refined query: '{refined_query}'")

# 6️⃣ Check for verse or chapter mention
verse_match = re.search(r'bg[:\- ]?\s*(\d+)[.: ](\d+)', refined_query, re.IGNORECASE)
chapter_match = re.search(r'(?:chapter|ch)\s*(\d+)', refined_query, re.IGNORECASE)

retrieved_context = ""


if verse_match:
    chapter = verse_match.group(1)
    verse = verse_match.group(2)
    verse_pattern = f"Bg. {chapter}.{verse}"
    print(f"🔎 Exact verse requested: {verse_pattern}")
    results = [entry for entry in structured_metadata if entry['verse_id'] == verse_pattern]

    if results:
        retrieved_context = f"{results[0]['verse_id']} — {results[0]['verse_text']} — {results[0]['translation']} — {results[0]['purport']}"
    else:
        # Fall back to semantic search
        query_vector = model.encode([refined_query])
        D, I = index.search(query_vector, k=3)
        results = [formatted_metadata[i] for i in I[0]]
        retrieved_context = "\n\n".join(results)

elif chapter_match:
    chapter = chapter_match.group(1)
    print(f"📖 Chapter requested: {chapter}")
    results = [entry for entry in structured_metadata if entry['verse_id'].startswith(f"Bg. {chapter}.")]

    if results:
        # Semantic search within the chapter
        chapter_verses = [
            f"{entry['verse_id']} — {entry['verse_text']} — {entry['translation']} — {entry['purport']}"
            for entry in results
        ]
        chapter_vectors = model.encode(chapter_verses)
        query_vector = model.encode([refined_query])
        scores = np.dot(chapter_vectors, np.array(query_vector).T)
        top_indices = scores.flatten().argsort()[-5:][::-1]
        top_verses = [chapter_verses[i] for i in top_indices]
        

        retrieved_context = "\n\n".join(top_verses)
        retrieved_context += f"\n\n(Chapter {chapter} has {len(results)} verses in total)"
    else:
        # Fall back to semantic search
        query_vector = model.encode([refined_query])
        D, I = index.search(query_vector, k=3)
        results = [formatted_metadata[i] for i in I[0]]
        retrieved_context = "\n\n".join(results)

else:
    # 7️⃣ Semantic vector search
    query_vector = model.encode([refined_query])
    D, I = index.search(query_vector, k=3)
    results = [formatted_metadata[i] for i in I[0]]
    retrieved_context = "\n\n".join(results)
    

# 8️⃣ STAGE 2: Generate User-Friendly Response
response_generation = client.chat.completions.create(
    messages=[
        {
            "role": "system",
            "content": (
                "You are a Bhagavad Gita expert assistant that provides accurate and helpful information. "
                "Your role is to format and present the retrieved information in a clear, organized manner while "
                "strictly adhering to the following rules:\n\n"
                "1. ONLY use information from the provided context\n"
                "2. NEVER add information, interpretations, or personal opinions not found in the context\n"
                "3. If information is not in the context, clearly state 'The context does not provide information about this'\n"
                "4. Format your response with proper headings, paragraph breaks, and bullet points when appropriate\n"
                "5. Include verse references (e.g., Bg. 2.47) when quoting or referring to specific verses\n"
                "6. Use non-technical language when possible, but maintain the spiritual integrity of the content\n"
                "7. If the context contains multiple verses, synthesize the information without omitting key points\n\n"
                "Remember: Accuracy is paramount. Never sacrifice factual correctness for readability."
            )
        },
        {
            "role": "user", 
            "content": f"Context from Bhagavad Gita:\n\n{retrieved_context}\n\nUser question: {original_query}\n\nProvide a clear, well-formatted answer based only on this context:"
        },
    ],
    model="llama3-70b-8192",
    temperature=0.2,
    top_p=0.2,
    max_tokens=800  # Adjust up to ~6000 if you want
)

final_answer = response_generation.choices[0].message.content

# 9️⃣ Output
print("\n📜 Retrieved Context Snippet:")
print(retrieved_context[:300], "...\n")

print("🧘 Final Answer:")
print(final_answer)

# 🔟 Optional: Save results
with open("rag_query_results.txt", "w") as f:
    f.write(f"Original Query: {original_query}\n")
    f.write(f"Refined Query: {refined_query}\n\n")
    f.write(f"Retrieved Context:\n{retrieved_context}\n\n")
    f.write(f"Final Answer:\n{final_answer}\n")



