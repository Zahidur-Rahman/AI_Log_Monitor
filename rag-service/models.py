from pydantic import BaseModel, Field
from typing import List, Dict, Any, TypedDict, Annotated
from langchain_core.messages import BaseMessage
import operator

# --- API Models ---

class QueryRequest(BaseModel):
    """The request model for the conversational query endpoint."""
    query: str = Field(..., min_length=1, description="The user's question.")
    conversation_id: str = Field(..., description="A unique ID to track the conversation.")

class QueryResponse(BaseModel):
    """The response model for the conversational query endpoint."""
    answer: str
    source_documents: List[Dict[str, Any]]

# --- LangGraph Agent State ---

class AgentState(TypedDict):
    """
    Defines the state of our agent. This is the "memory" that passes between nodes.
    
    Attributes:
        messages: The history of messages in the conversation.
        documents: The list of documents retrieved from the vector store.
    """
    messages: Annotated[List[BaseMessage], operator.add]
    documents: List[Dict[str, Any]]