# from langchain_community.document_loaders import WebBaseLoader
# from langchain_text_splitters import RecursiveCharacterTextSplitter

# # 🌐 Load a Sanskrit webpage (example below is a Sanskrit text from Wikipedia)
# url = "https://sa.wikipedia.org/wiki/गीता"
# loader = WebBaseLoader(url)
# docs = loader.load()

# # 📄 Display first few characters to check if it's Sanskrit
# print(docs[0].page_content[:500])  # Just preview

# # ✂️ Split the Sanskrit text into manageable chunks
# text_splitter = RecursiveCharacterTextSplitter(chunk_size=300, chunk_overlap=30)
# chunks = text_splitter.split_documents(docs)

# # 🧩 Show first chunk
# print("\n--- First Chunk ---\n")
# print(chunks[0].page_content)

from langchain_community.document_loaders import WebBaseLoader  
from langchain_text_splitters import RecursiveCharacterTextSplitter  
# from langchain_community.llms import Ollama
from langchain_ollama import OllamaLLM
from langchain.chains.question_answering import load_qa_chain

# 1. Load the web page
loader = WebBaseLoader("https://lilianweng.github.io/posts/2023-06-23-agent/")
docs = loader.load()

# 2. Split the text
splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)
docs_split = splitter.split_documents(docs)

# 3. Initialize Ollama with your local model
llm = OllamaLLM(model="mistral")  # Or use 'mistral', 'llama2', etc. if installed

# 4. Create a simple QA chain (you can replace this with summarization or chat)
chain = load_qa_chain(llm, chain_type="stuff")

# 5. Ask a question to the chain
query = "What are the main points discussed in the article?"
response = chain.run(input_documents=docs_split, question=query)

print(response)

