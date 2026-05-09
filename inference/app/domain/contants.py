from enum import Enum

DEFAULT_K_NEAREST_NEIGHBORS = 50
DEFAULT_TOP_ITEMS = 5

class WorkflowMode(Enum):
    SWITCH = "switch"
    DIRECT = "direct"
    HANDOFF = "handoff"
    MAGENTIC = "magentic"

class TypeDeploymentClient(Enum):
    AZURE = "azure"
    OPEN_AI = "open_ai"
    AI_FOUNRY = "ai_foundry"
    AGENT_FRAMEWORK = "agent_framework"
    EXTERNAL_AI_FOUNDRY = "external_ai_foundry"

DISPATCHER_DEFAULT_NAME = "DispatcherAgent"
DISPATCHER_GEN_IA_NAME = "GenIaAgent"

CHECKPOINTS_DIRECTORY = "./checkpoints"

class DecisionAction(Enum):
    ACCEPT = "Accept"
    REJECT = "Reject"

MEDIA_FILE_MAPPER = {
    'pdf': 'application/pdf',
    'jpg': 'image/jpeg',
    'png': 'image/png',
    'jpeg': 'image/jpeg',
    'pptx': 'application/octet-stream',
    'docx': 'application/octet-stream',
    'txt': 'application/octet-stream'
}

class LlmProviderEnum(Enum):
    OPEN_AI = "Open Ai"
    ANTHROPIC = "GANTHROPICP"
    DEEP_SEEK = "GDEEP_SEEKP"

DEFAULT_DT_FORMAT = "%d/%m/%Y %H:%M"