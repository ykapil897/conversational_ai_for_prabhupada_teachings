import json
import chromadb
from chromadb.utils import embedding_functions
from sentence_transformers import SentenceTransformer

# Load the JSON
with open("gita_verses_progress.json", "r") as f:
    data = json.load(f)

# Prepare embedding model
model = SentenceTransformer("all-MiniLM-L6-v2")
def embed(texts): return model.encode(texts).tolist()

# Create ChromaDB client and collection
client = chromadb.PersistentClient(path="./chroma_db")
collection = client.create_collection(name="gita_verses")

# Prepare and insert data
docs, ids, metadatas = [], [], []

for item in data:
    content = f"""{item['verse_id']}\n{item['devanagari']}\n{item['verse_text']}\n{item['translation']}\n{item['purport']}"""
    docs.append(content)
    ids.append(item['verse_id'])
    metadatas.append({
        "url": item["url"],
        "verse_id": item["verse_id"]
    })

collection.add(documents=docs, ids=ids, embeddings=embed(docs), metadatas=metadatas)

print("✅ Data loaded into ChromaDB.")
