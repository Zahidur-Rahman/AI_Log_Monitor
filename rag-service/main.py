import logging
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from langchain_core.messages import HumanMessage, AIMessage

from models import QueryRequest, QueryResponse, AgentState
from agent import create_agent_executor

# --- Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- FastAPI App ---
app = FastAPI(title="Conversational RAG Service")
agent_executor = create_agent_executor()

# A simple in-memory dictionary to store conversation states.
# In a production app, you would use a database like Redis.
conversation_states = {}

# --- API Endpoints ---
@app.get("/")
def read_root():
    return {"status": "Conversational RAG Service is running"}

@app.post("/rag/query", response_model=QueryResponse, summary="Query logs with a conversational agent")
async def query_logs(request: QueryRequest):
    """
    Accepts a query and a conversation ID to maintain a stateful dialogue.
    """
    conversation_id = request.conversation_id
    
    # Get the existing state for this conversation or create a new one
    current_state = conversation_states.get(conversation_id, AgentState(messages=[], documents=[]))
    
    # Add the new user message to the state
    current_state['messages'].append(HumanMessage(content=request.query))
    
    # Invoke the agent
    final_state = agent_executor.invoke(current_state)
    
    # Update the stored state with the full history
    conversation_states[conversation_id] = final_state
    
    # Extract the final answer and sources for the response
    final_answer = final_state['messages'][-1].content
    source_documents = final_state['documents']
    
    return {"answer": final_answer, "source_documents": source_documents}