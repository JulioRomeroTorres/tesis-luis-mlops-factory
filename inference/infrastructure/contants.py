from enum import Enum
from typing import Dict
from pydantic import BaseModel

class WorkflowMode(Enum):
    SWITCH = "switch"
    DIRECT = "direct"
    HANDOFF = "handoff"
    MAGENTIC = "magentic"

class AgentMetadata(BaseModel):
    name: str
    id: str

class AgentType(Enum):
    DISPATCHER = "dispatcher"
    GEN_IA = "gen_ia"
    FROM_CATALOG= "agents_from_catalog"

class AgentClassificationLabel(Enum):
    GEN_IA = "gen_ia_agent"
    FROM_CATALOG = "from_catalog_agent"

AGENTS_METADATA: Dict[AgentType, AgentMetadata] = {
    AgentType.DISPATCHER: AgentMetadata(name="Dispatcher", id="dispatcher_agent"),
    AgentType.GEN_IA: AgentMetadata(name="GenIaAgent", id="gen_ia_agent"),
    AgentType.FROM_CATALOG: AgentMetadata(name="CatalogAgent", id="agents_from_catalog")
}