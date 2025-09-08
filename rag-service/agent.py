import os
import json
from typing import List
from dotenv import load_dotenv

from langchain_core.messages import HumanMessage
from langchain.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langgraph.graph import StateGraph, END

from models import AgentState

# --- Load Environment Variables ---
load_dotenv()
CHROMA_HOST = os.getenv("CHROMA_HOST", "chroma")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

# --- Initialize Connections ---
embedding_function = SentenceTransformerEmbeddings(model_name='all-MiniLM-L6-v2')

# Connect to ChromaDB
import chromadb
chroma_client = chromadb.HttpClient(host=CHROMA_HOST, port=8000)
vector_store = Chroma(
    collection_name="logs",
    embedding_function=embedding_function,
    client=chroma_client
)
retriever = vector_store.as_retriever(search_kwargs={"k": 10})
llm = ChatGroq(temperature=0, groq_api_key=GROQ_API_KEY, model_name="llama-3.1-8b-instant")

# --- LangGraph Nodes ---

def retrieve_documents(state: AgentState) -> AgentState:
    """
    Node to retrieve documents from the vector store using full conversation context.
    """
    print("---NODE: RETRIEVE DOCUMENTS---")
    
    # Build search query using last 3 messages as context
    last_3_messages = state['messages'][-3:] if len(state['messages']) >= 3 else state['messages']
    
    # Extract content from last 3 messages
    context_parts = []
    for msg in last_3_messages:
        role = "User" if hasattr(msg, 'type') and msg.type == 'human' else "Assistant"
        context_parts.append(f"{role}: {msg.content}")
    
    # Use the latest message as primary query with last 3 messages as context
    latest_query = state['messages'][-1].content
    
    if len(last_3_messages) > 1:
        context_query = f"Recent conversation: {' | '.join(context_parts[:-1])}. Current query: {latest_query}"
    else:
        context_query = latest_query
    
    print(f"Searching with context: {context_query[:100]}...")
    
    # Retrieve documents with enhanced context
    retrieved_docs = retriever.get_relevant_documents(context_query)
    
    # Convert LangChain Document objects to simple dictionaries for the state
    documents_as_dicts = [{"page_content": doc.page_content, "metadata": doc.metadata} for doc in retrieved_docs]
    return {"documents": documents_as_dicts, "messages": []}

def generate_response(state: AgentState) -> AgentState:
    """
    Node to generate a response from the LLM using the full conversation history and retrieved documents.
    """
    print("---NODE: GENERATE RESPONSE---")
    prompt_template = """
    You are a highly intelligent log analysis assistant with contextual memory. Answer the user's question based on the recent conversation history (last 3 messages) and retrieved log entries.

    RECENT CONVERSATION HISTORY (Last 3 Messages):
    {chat_history}

    RETRIEVED LOGS (based on recent context):
    {context}

    INSTRUCTIONS:
    1. Consider the recent conversation context (last 3 messages) to understand the user's intent
    2. Reference recent questions and answers when relevant
    3. Build upon recent analysis and findings
    4. If this is a follow-up question, connect it to the recent conversation
    5. Provide specific log details and timestamps when available
    6. If information is missing, explain what you found instead

    CURRENT USER QUESTION:
    {question}

    CONTEXTUAL ANSWER:
    """
    prompt = ChatPromptTemplate.from_template(prompt_template)
    
    current_question = state['messages'][-1].content
    
    # Build chat history using last 3 messages (excluding current)
    last_3_messages = state['messages'][-4:-1] if len(state['messages']) >= 4 else state['messages'][:-1]
    
    chat_history_parts = []
    for msg in last_3_messages:
        role = "User" if hasattr(msg, 'type') and msg.type == 'human' else "Assistant"
        chat_history_parts.append(f"{role}: {msg.content}")
    
    chat_history = "\n".join(chat_history_parts) if chat_history_parts else "No previous conversation"
    
    # Format retrieved documents with better structure
    context_parts = []
    for i, doc in enumerate(state['documents'], 1):
        context_parts.append(f"Log Entry {i}: {doc['page_content']}")
    context = "\n\n".join(context_parts) if context_parts else "No relevant logs found"
    
    print(f"Generating response with {len(state['messages'])} messages in history")

    chain = prompt | llm
    response = chain.invoke({
        "chat_history": chat_history,
        "context": context,
        "question": current_question,
    })
    
    return {"messages": [response], "documents": []}

def rewrite_query(state: AgentState) -> AgentState:
    """
    Node to rewrite the user's query to be a standalone query based on full conversation history.
    """
    print("---NODE: REWRITE QUERY---")
    latest_message = state['messages'][-1]
    
    # If this is the first message, no rewriting needed
    if len(state['messages']) == 1:
        print("First message - no rewriting needed")
        return {"messages": [], "documents": []}

    rewrite_prompt = ChatPromptTemplate.from_messages([
        ("human", """Given this conversation history about log analysis:

{chat_history}

Rewrite this follow-up question to be a complete, standalone query that includes all necessary context: '{question}'

Standalone query:""")
    ])

    rewriter_chain = rewrite_prompt | llm
    
    # Build chat history using last 3 messages for rewriting context
    last_3_messages = state['messages'][-4:-1] if len(state['messages']) >= 4 else state['messages'][:-1]
    
    chat_history_parts = []
    for msg in last_3_messages:
        role = "User" if hasattr(msg, 'type') and msg.type == 'human' else "Assistant"
        chat_history_parts.append(f"{role}: {msg.content}")
    
    chat_history = "\n".join(chat_history_parts)
    
    print(f"Rewriting query with last {len(last_3_messages)} messages as context")
    
    rewritten_question = rewriter_chain.invoke({
        "chat_history": chat_history,
        "question": latest_message.content
    })
    
    # Replace the latest message with the rewritten version
    new_message = HumanMessage(content=rewritten_question.content)
    all_messages = state['messages'][:-1] + [new_message]
    
    print(f"Original: {latest_message.content}")
    print(f"Rewritten: {rewritten_question.content}")
    
    return {"messages": all_messages, "documents": []}

# --- Define the Graph ---

def create_agent_executor():
    """
    Compiles the nodes and edges into a runnable LangGraph agent.
    """
    workflow = StateGraph(AgentState)

    workflow.add_node("rewrite_query", rewrite_query)
    workflow.add_node("retrieve", retrieve_documents)
    workflow.add_node("generate", generate_response)

    workflow.set_entry_point("rewrite_query")
    workflow.add_edge("rewrite_query", "retrieve")
    workflow.add_edge("retrieve", "generate")
    workflow.add_edge("generate", END)

    return workflow.compile()