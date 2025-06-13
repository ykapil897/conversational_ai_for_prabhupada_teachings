import os
import faiss
import json
import re
from sentence_transformers import SentenceTransformer
from groq import Groq

# 1️⃣ Load embedding model
model = SentenceTransformer("BAAI/bge-large-en")

# 2️⃣ Load FAISS index
index = faiss.read_index("gita_index.faiss")

# 3️⃣ Load metadata (structured and formatted)
with open("gita_metadata.json", "r") as f:
    structured_metadata = json.load(f)

# Create formatted metadata with clear section separation for source attribution AND clickable URLs
formatted_metadata = []
for entry in structured_metadata:
    verse_part = f"VERSE: {entry['verse_text']}" if 'verse_text' in entry else ""
    translation_part = f"TRANSLATION: {entry['translation']}" if 'translation' in entry else ""
    purport_part = f"PURPORT BY SRILA PRABHUPADA: {entry['purport']}" if 'purport' in entry else ""
    url_part = f"URL: {entry['url']}" if 'url' in entry else ""
    
    formatted_text = f"REFERENCE: {entry.get('verse_id', 'Unknown verse')}\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\n{url_part}"
    formatted_metadata.append(formatted_text)

# 4️⃣ Setup Groq
client = Groq(api_key=os.environ.get("GROQ_API_KEY", "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"))

def process_query(original_query):
    """Process a single query through the entire RAG pipeline with source attribution"""
    
    # 5️⃣ STAGE 1: Query Refinement with LLM
    print(f"🔍 Original query: '{original_query}'")

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
                )
            },
            {
                "role": "user",
                "content": f"Please refine this query for searching the Bhagavad Gita: '{original_query}'"
            },
        ],
        model="llama3-70b-8192",
        temperature=0.2,
        top_p=0.2,
        max_tokens=100
    )

    refined_query = query_refinement_response.choices[0].message.content.strip()
    refined_query = re.sub(r'^"(.+)"$', r'\1', refined_query)  # Remove quotes if the LLM added them
    print(f"🔄 Refined query: '{refined_query}'")

    # 6️⃣ Check for verse or chapter mention
    verse_match = re.search(r'bg[:\- ]?\s*(\d+)[.: ](\d+)', refined_query, re.IGNORECASE)
    chapter_match = re.search(r'(?:chapter|ch)\s*(\d+)', refined_query, re.IGNORECASE)

    retrieved_context = ""
    retrieval_sources = []

    if verse_match:
        chapter = verse_match.group(1)
        verse = verse_match.group(2)
        verse_pattern = f"Bg. {chapter}.{verse}"
        print(f"🔎 Exact verse requested: {verse_pattern}")
        results = [entry for entry in structured_metadata if entry.get('verse_id') == verse_pattern]
        
        if results:
            # Format with clear section markers for source attribution
            verse_entry = results[0]
            verse_part = f"VERSE: {verse_entry.get('verse_text', '')}"
            translation_part = f"TRANSLATION: {verse_entry.get('translation', '')}"
            purport_part = f"PURPORT BY SRILA PRABHUPADA: {verse_entry.get('purport', '')}"
            url_part = f"URL: {verse_entry.get('url', '')}"
            
            retrieved_context = f"REFERENCE: {verse_entry.get('verse_id', '')}\n\n{verse_part}\n\n{translation_part}\n\n{purport_part}\n\n{url_part}"
            
            retrieval_sources = [{
                "verse_id": verse_entry.get('verse_id', ''),
                "has_verse": 'verse_text' in verse_entry and bool(verse_entry['verse_text']),
                "has_translation": 'translation' in verse_entry and bool(verse_entry['translation']),
                "has_purport": 'purport' in verse_entry and bool(verse_entry['purport']),
                "url": verse_entry.get('url', '')
            }]
        else:
            # Fall back to semantic search if verse not found
            query_vector = model.encode([refined_query])
            D, I = index.search(query_vector, k=3)
            results = [formatted_metadata[i] for i in I[0]]
            retrieved_context = "\n\n---\n\n".join(results)
            
            # Track sources used in semantic search
            for i in I[0]:
                source_entry = structured_metadata[i]
                retrieval_sources.append({
                    "verse_id": source_entry.get('verse_id', 'Unknown verse'),
                    "has_verse": 'verse_text' in source_entry and bool(source_entry['verse_text']),
                    "has_translation": 'translation' in source_entry and bool(source_entry['translation']),
                    "has_purport": 'purport' in source_entry and bool(source_entry['purport']),
                    "url": source_entry.get('url', '')
                })

    elif chapter_match:
        chapter = chapter_match.group(1)
        print(f"📖 Chapter requested: {chapter}")
        results = [entry for entry in structured_metadata if entry.get('verse_id', '').startswith(f"Bg. {chapter}.")]
        
        if results:
            # For chapter queries, include clear section markers
            chapter_texts = []
            for entry in results[:5]:  # Limit to 5 verses for readability
                verse_id = entry.get('verse_id', 'Unknown verse')
                translation = entry.get('translation', '')
                url = entry.get('url', '')
                chapter_texts.append(f"REFERENCE: {verse_id}\n\nTRANSLATION: {translation}\n\nURL: {url}")
                
                retrieval_sources.append({
                    "verse_id": verse_id,
                    "has_verse": False,
                    "has_translation": True,
                    "has_purport": False,
                    "url": url
                })
            
            retrieved_context = "\n\n---\n\n".join(chapter_texts)
            retrieved_context += f"\n\n(Chapter {chapter} has {len(results)} verses in total)"
        else:
            # Fall back to semantic search
            query_vector = model.encode([refined_query])
            D, I = index.search(query_vector, k=3)
            results = [formatted_metadata[i] for i in I[0]]
            retrieved_context = "\n\n---\n\n".join(results)
            
            for i in I[0]:
                source_entry = structured_metadata[i]
                retrieval_sources.append({
                    "verse_id": source_entry.get('verse_id', 'Unknown verse'),
                    "has_verse": 'verse_text' in source_entry and bool(source_entry['verse_text']),
                    "has_translation": 'translation' in source_entry and bool(source_entry['translation']),
                    "has_purport": 'purport' in source_entry and bool(source_entry['purport']),
                    "url": source_entry.get('url', '')
                })

    else:
        # 7️⃣ Semantic vector search
        query_vector = model.encode([refined_query])
        D, I = index.search(query_vector, k=3)
        results = [formatted_metadata[i] for i in I[0]]
        retrieved_context = "\n\n---\n\n".join(results)
        
        for i in I[0]:
            source_entry = structured_metadata[i]
            retrieval_sources.append({
                "verse_id": source_entry.get('verse_id', 'Unknown verse'),
                "has_verse": 'verse_text' in source_entry and bool(source_entry['verse_text']),
                "has_translation": 'translation' in source_entry and bool(source_entry['translation']),
                "has_purport": 'purport' in source_entry and bool(source_entry['purport']),
                "url": source_entry.get('url', '')
            })

    # 8️⃣ STAGE 2: Extract Key Facts and Citations
    facts_extraction = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a Bhagavad Gita facts extractor. Your job is to carefully extract important information from the context "
                    "with precise attribution and URLs. Follow these guidelines:\n\n"
                    
                    "1. BEGIN with a one-sentence acknowledgment of Srila Prabhupada's contribution.\n\n"
                    
                    "2. Extract 3-5 key facts or teachings that address the user's question.\n\n"
                    
                    "3. For each fact:\n"
                    "   - Include the exact verse reference (e.g., Bg. 2.47)\n"
                    "   - Include direct quotes from the verse, translation, or purport\n"
                    "   - Specify the source (verse, translation or purport)\n"
                    "   - Keep the original wording intact\n"
                    "   - Include the URL for the verse (e.g., https://vedabase.io/en/library/bg/2/47/)\n\n"
                    
                    "4. Format each fact as: \"FACT: [brief description] | SOURCE: [verse reference and type] | QUOTE: [exact quote] | URL: [clickable link]\"\n\n"
                    
                    "5. ONLY include information directly from the provided context.\n\n"
                    
                    "6. Do NOT interpret or expand beyond what's explicitly stated in the context.\n\n"
                    
                    "Keep your extraction focused on the most relevant facts for the user's question."
                )
            },
            {
                "role": "user", 
                "content": f"Context from Bhagavad Gita:\n\n{retrieved_context}\n\nUser question: {original_query}\n\nExtract key facts with proper attribution and URLs:"
            },
        ],
        model="llama3-70b-8192",
        temperature=0.2,
        top_p=0.2,
        max_tokens=800
    )

    extracted_facts = facts_extraction.choices[0].message.content
    
    # Parse extracted facts to ensure we can reference them in the final answer
    parsed_facts = []
    fact_pattern = re.compile(r'FACT: (.*?) \| SOURCE: (.*?) \| QUOTE: "(.*?)" \| URL: (.*?)$', re.MULTILINE)
    for match in fact_pattern.finditer(extracted_facts):
        parsed_facts.append({
            "description": match.group(1),
            "source": match.group(2),
            "quote": match.group(3),
            "url": match.group(4)
        })

    # 9️⃣ STAGE 3: Transform into User-Friendly Explanation WHILE PRESERVING QUOTES
    user_friendly_response = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an expert on Bhagavad Gita who transforms factual information into a warm, accessible explanation "
                    "that feels like Srila Prabhupada is directly guiding the reader. Follow these guidelines:\n\n"
                    
                    "1. START with a humble greeting that acknowledges Srila Prabhupada once at the beginning and 'Hare Krishna' for the devotees in the beginning only.\n\n"
                    
                    "2. TRANSFORM the extracted facts into a cohesive, conversational explanation that feels personal.\n\n"
                    
                    "3. ORGANIZE the content with clear visual structure:\n"
                    "   - Use elegant section headings (marked with 🕉️ or ✨)\n"
                    "   - Highlight key concepts in **bold**\n"
                    "   - Use thoughtful paragraph breaks for readability\n\n"
                    
                    "4. INCLUDE DIRECT QUOTES from the extracted facts - this is CRITICAL:\n"
                    "   - Use the exact quotes provided in the facts\n"
                    "   - Format quotes with quotation marks and include the verse reference\n"
                    "   - Add verse references as clickable links [Bg. X.Y](URL)\n\n"
                    
                    "5. MAINTAIN complete accuracy to the source material - don't add new interpretations.\n\n"
                    
                    "6. USE language that feels warm, direct, and instructive - as if speaking directly to the devotee.\n\n"
                    
                    "7. CONCLUDE with a practical application or reflection that helps the reader connect the teaching to their life.\n\n"
                    
                    "8. AVOID repeatedly mentioning Srila Prabhupada by name after the introduction.\n\n"
                    
                    "Your goal is to create a reading experience that feels like sitting at the lotus feet of the spiritual master, "
                    "receiving direct instruction - while maintaining complete fidelity to the original teachings and PRESERVING THE EXACT QUOTES."
                )
            },
            {
                "role": "user", 
                "content": f"Here are the extracted facts with attributions and URLs:\n\n{extracted_facts}\n\nUser question: {original_query}\n\nTransform these facts into a beautiful, user-friendly explanation that feels like direct guidance from Srila Prabhupada, INCLUDING THE EXACT QUOTES from the extracted facts:"
            },
        ],
        model="llama3-70b-8192",  
        temperature=0.4,
        top_p=0.5,
        max_tokens=1000
    )

    final_answer = user_friendly_response.choices[0].message.content

    # 🔟 Output
    print("\n📜 Retrieved Context Snippet:")
    print(retrieved_context[:300], "...\n")  # Print a snippet if long

    print("📝 Extracted Facts:")
    print(extracted_facts)
    
    print("\n🧘 Final User-Friendly Answer:")
    print(final_answer)

    # Save the results for analysis
    with open("rag_query_results.txt", "w") as f:
        f.write(f"Original Query: {original_query}\n")
        f.write(f"Refined Query: {refined_query}\n\n")
        f.write(f"Retrieved Context:\n{retrieved_context}\n\n")
        f.write(f"Extracted Facts:\n{extracted_facts}\n\n")
        f.write(f"Final Answer:\n{final_answer}\n")
        f.write(f"\nSource Citations:\n")
        for source in retrieval_sources:
            f.write(f"- {source['verse_id']}: " + 
                   f"Verse: {'✓' if source['has_verse'] else '✗'}, " +
                   f"Translation: {'✓' if source['has_translation'] else '✗'}, " +
                   f"Purport: {'✓' if source['has_purport'] else '✗'}, " +
                   f"URL: {source['url']}\n")
    
    return {
        "original_query": original_query,
        "refined_query": refined_query,
        "retrieved_context": retrieved_context,
        "extracted_facts": extracted_facts,
        "final_answer": final_answer,
        "retrieval_sources": retrieval_sources
    }

# Make the function available for import
if __name__ == "__main__":
    # Example usage
    query = "What does the Gita say about duty and action?"
    process_query(query)