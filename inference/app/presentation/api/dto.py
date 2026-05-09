import re
from typing import (
    Any, Optional, List,
    Dict, Any, Literal, Annotated
)
from fastapi import UploadFile
from pydantic import BaseModel, Field, model_validator, field_validator
from app.domain.utils import get_current_datetime
from app.domain.agent.workflow import AgenticEdge, AgenticNode
from app.application.constants import VALID_EVALUATION_FILES
from app.domain.utils import parse_comma_separated

from enum import Enum

class OrderEnum(Enum):
    ASC = 'ASC'
    DESC = 'DESC'

class Message(BaseModel):
    role: str = Field(description="Role of the message sender (user, assistant, system)")
    content: str = Field(description="Message content")

class AgentTrace(BaseModel):
    identity_id: Optional[str] = Field(
        default=None,
        description="User id"
    )
    identity_type: Optional[str] = Field(
        default=None,
        description="Bussiness Unit"
    )
    identity_domain: Optional[str] = Field(
        default=None,
        description="Bussiness domian"
    )
    identity_subdomain: Optional[str] = Field(
        default=None,
        description="Bussiness subdomian"
    )
    
    identity_parent_session_id:  Optional[str] = Field(
        default=None,
        description="Interaction id"
    )

    def to_json(self)-> Dict[str, Any]:
        return {
            "identity.id": self.identity_id,
            "identity.type": self.identity_type,
            "identity.domain": self.identity_domain,
            "identity.subdomain": self.identity_subdomain
        }

class CommonParameterResource(BaseModel):
    name: str = Field(decription="Resource Name")

    @field_validator('name')
    @classmethod
    def validar_nombre(cls, v: str) -> str:
        patron = r'^[a-zA-Z0-9\s\-\_\.]+$'
        
        if not re.match(patron, v):
            raise ValueError('El nombre solo puede contener letras, números, espacios y puntos')
        
        if re.search(r'\s{2,}', v):
            raise ValueError('El nombre no puede tener espacios consecutivos')
        
        if v.startswith(' ') or v.endswith(' '):
            raise ValueError('El nombre no puede empezar o terminar con espacios')
        
        return v.strip()

class AgentInformationRequest(CommonParameterResource):
    model: Optional[str] = Field(decription="Model Name", default="gpt-4o-mini") 
    system_instruction: Optional[str] = Field(description="Prompt Agent", default="")
    description: Optional[str] = Field(description="Agent Description", default="")
    version: Optional[str] = Field(description="Agent Version", default="v1")
    tools_ids: Optional[List[str]] = Field(description="Tools Ids", default=[])
    enable_memory: Optional[bool] = Field(description="Enable Long Memory", default= False)
    guardrails_ids: Optional[List[str]] = Field(description="Guardrails Ids", default=[])

class ToolInformationRequest(CommonParameterResource):
    alias: str = Field(decription="Tool Name")
    description: str = Field(description="Tool Description")
    input_params: Optional[Dict[str, Any]] = Field(description="Input Params", default=None)
    logic_content: str = Field(description="Logic content of tool")

class GuardrailInformationRequest(CommonParameterResource):
    description: str = Field(description="Guardrail Description")
    severity_scale: Literal[4,8] = Field(description="Scale of severity", default=4)
    tunning_parameters: Dict[str, int] = Field(description="Threshold of topics")
    modes: List[str] = Field(description="Modes List where the guardrail will be applied")

class WorkflowExecutionConfig(BaseModel):
    max_iterations: Optional[int] = 20
    checkpoint_enabled: Optional[bool] = False

class WorkflowInformationRequest(CommonParameterResource):
    unique_agents_ids: Optional[List[str]] = Field(description="Workflow description", default=[])
    description: Optional[str] = Field(description="Workflow description", default=None)
    start_node: Optional[str] = Field(decription="Start Workflow Node", default=None)
    execution_config: Optional[WorkflowExecutionConfig] = Field(decription="Workflow Settings", default=WorkflowExecutionConfig()) 
    nodes: Optional[List[AgenticNode]] = Field(description="Prompt Agent", default=[])
    edges: Optional[List[AgenticEdge]] = Field(description="Prompt Agent", default=[])
    is_free_graph: Optional[bool] = Field(description="Is Free Graph", default=True)
    
class PrimitiveConversationInformation(BaseModel):
    message: str = Field(description="Current user message")
    additional_files: Optional[List[str]] = Field(
        default_factory=list,
        description="List of attached files"
    )

class ConversationRequest(PrimitiveConversationInformation):
    trace: AgentTrace = Field(
        description="trace information"
    )
    additional_information: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional Params"
    )
    
class ConversationResponse(BaseModel):
    type: str = Field(description="Response type", default="text")
    content: str = Field(description="Response message")
    timestamp: str = Field(description="Response timestamp", default=get_current_datetime())
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

class AgentResponse(BaseModel):
    type: str = Field(description="Response type", default="text")
    value: str = Field(description="Response message")
    author: str = Field(description="Agent Name")

class WorkflowResponse(BaseModel):
    contents: List[AgentResponse] = Field(description="Response message", default=[])
    timestamp: str = Field(description="Response timestamp", default=get_current_datetime())
    metadata: dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

class CommonFilterParams(BaseModel):
    page: Optional[int] = 0
    limit: Optional[int] = 10
    order: Optional[str] = OrderEnum.DESC.value

class ConversationFilters(CommonFilterParams):
    pass
    
class UploadedDocumentResponse(BaseModel):
    generated_img_files: List[str] = Field(description="List of image file from pdf page")
    file_name: str = Field(description="List of file name")

class CreationToolByDescriptionRequest(BaseModel):
    description: str = Field(description="Complete description of the tool will be created")

class EvaluationInformationRequest(BaseModel):
    agent_id: str = Field(description="Agent Id to create evaluation")
    

class CreateEvaluationRequest(BaseModel):
    file: UploadFile
    name: str
    description: Optional[str] = None
    dataset_id: Optional[str] = None
    selected_evaluators: List[str] = Field(
        min_length=1,
        max_length=5,
        default=[],
        description="Lista de evaluadores"
    )
    agents_ids: List[str] = Field(
        min_length=1,
        max_length=10,
        default=[],
        description="Lista de evaluadores"
    )

    @model_validator(mode='before')
    @classmethod
    def transform_field(cls, data: Any) -> Any:

        if ("dataset_id" in data) and (data["dataset_id"] in ["undefined", "null", ""]) :
            data["dataset_id"] = None


        if "selected_evaluators" in data:
            data["selected_evaluators"] = parse_comma_separated(
                    data["selected_evaluators"], "selected_evaluators"
            )
        
        if "agents_ids" in data:
            data["agents_ids"] = parse_comma_separated(
                    data["agents_ids"], "selected_evaluators"
            )

        return data

    @field_validator('file')
    @classmethod
    def passwords_coinciden(cls, v: UploadFile) -> UploadFile:

        if v.size > 50*1024:
            raise ValueError(f"El tamaño del archivo debe de ser menor o igual a 50 KB")
        
        if v.content_type not in VALID_EVALUATION_FILES:
            raise ValueError(f"El contenido del archivo solo puede ser {",".join(VALID_EVALUATION_FILES)}, sin embargo este es {v.content_type}")
        
        return v

