import logging
from typing import (
    List, Any, 
    Dict, Optional, AsyncIterable
) 

from app.domain.utils import get_metadata_from_uri
from app.domain.agent.agent import (
    AgentSettings, WorkflowSettings, 
    IntentClassification
)

from agent_framework import (
    Message, Content
)

from app.infrastructure.agents.agnostic_agent import AgnosticAgent
from agent_framework import (
    WorkflowBuilder, 
    Case, Default, AgentExecutor,
    Workflow, WorkflowRunResult, WorkflowEvent
)

from agent_framework.orchestrations import (
    ConcurrentBuilder, SequentialBuilder
)

from app.infrastructure.executors.intent_classifier_executor import IntentClassifierExecutor
from app.domain.orchestrator.service import IWorkflowOrchestrator

from app.domain.agent.workflow import CompletedWorkflowInformation, AgenticNode
from app.infrastructure.executors.classified_agent_executor import ClassifiedAgentExecutor
from app.infrastructure.executors.named_agent_executor import NamedAgentExecutor

logger = logging.getLogger(__name__)

class WorkflowOrchestrator(IWorkflowOrchestrator):
    def __init__(
        self,
        db_client: Any
    ) -> None:
        
        self.agent_garden: Dict[str, Any] = {}
        self.workflow:Workflow = None
        self.db_client = db_client
        self.mapper_name = {}

    def create_agent(self, agent_settings: AgentSettings, conversation_id: Optional[str] = None) -> None:
        wrapper_agent = AgnosticAgent(conversation_id, agent_settings, self.db_client)

        self.agent_garden[agent_settings.id] = {
            "type": "agent",
            "name": agent_settings.name,
            "description": agent_settings.description,
            "content": wrapper_agent
        }

    def generate_switch_case_edge_group(self, agents_ids: List[str], default_agent_id: str, workflow_alias: str) -> List[Any]:
        
        switch_conditions = []
        
        default_target_executor = ClassifiedAgentExecutor(
            agent=self.agent_garden[default_agent_id].get("content").agent,
            id=f"{workflow_alias}_{self.agent_garden[default_agent_id].get("name")}_{len(agents_ids)+1}"
        )

        for index, agent_id in enumerate(agents_ids):
            current_agent = self.agent_garden[agent_id].get("content").agent
            name = self.agent_garden[agent_id].get("name")

            agent_executor = ClassifiedAgentExecutor(
                agent=current_agent,
                id=f"{workflow_alias}_{name}_{index}"
            )

            switch_conditions.append(
                Case(
                    condition=lambda msg, idx=index: isinstance(msg, IntentClassification) and msg.intent == name,
                    target=agent_executor
                )
            )

        return [*switch_conditions, Default(target=default_target_executor)]

    def generate_classifier_agent(self, workflow_id: str, agent_ids: List[str]):

        rules = [
            f"""
            -{self.agent_garden[agent_id]["name"]}: Si se trata de temas relacionados con {self.agent_garden[agent_id]["description"]}
            """
            for agent_id in agent_ids
        ]

        agent_settings = AgentSettings(
            name=f"classifier-agent-{workflow_id}",
            system_instruction=f"""
                Eres un agente especialista en clasificar la intencion del usuario
                en base a las siguientes categorias:
                {
                    '\n'.join(rules)
                }
                Como respuesta indicaras la etiqueta, la razon por lo etiquetaste así y el mensaje original que realizó el usuario
            """,
            tools=[],       
            version='v1',     
            model='gpt-4o-mini',
            id=f"{workflow_id}_classifier-agent"
        )
        agent_wrapper = AgnosticAgent(None, agent_settings, self.db_client)
        return agent_wrapper.agent

    def create_sub_workflow(self, sub_workflow_settings: WorkflowSettings): 

        agent_garden_type = "workflow"

        if sub_workflow_settings.sub_type == 'sequential':
            participants = [ 
                AgentExecutor(
                    self.agent_garden[agent_id].get("content").agent,
                    id=f"{sub_workflow_settings.id}_{self.agent_garden[agent_id].get("name")}_{index}"
                )
                for index, agent_id in enumerate(sub_workflow_settings.sub_agents) 
            ]
            self.agent_garden[sub_workflow_settings.id] = {
                "type": agent_garden_type,
                "name": sub_workflow_settings.id,
                "content": SequentialBuilder(participants=participants).build().as_agent(sub_workflow_settings.id)
            }

        if sub_workflow_settings.sub_type == 'parallel':
            participants = [ 
                AgentExecutor(
                    self.agent_garden[agent_id].get("content").agent,
                    id=f"{sub_workflow_settings.id}_{self.agent_garden[agent_id].get("name")}_{index}"
                )
                for index, agent_id in enumerate(sub_workflow_settings.sub_agents) 
            ]
            self.agent_garden[sub_workflow_settings.id] = {
                "type": agent_garden_type,
                "name": sub_workflow_settings.id,
                "content": ConcurrentBuilder(participants=participants).build().as_agent(sub_workflow_settings.id)
            } 

        if sub_workflow_settings.sub_type == 'switch':
            classifier_agent = self.generate_classifier_agent(sub_workflow_settings.id, sub_workflow_settings.sub_agents)
            classifier_executor = IntentClassifierExecutor(
                classifier_agent,
                sub_workflow_settings.id
            )

            builder = (
                WorkflowBuilder(start_executor=classifier_executor)
                .add_switch_case_edge_group(
                    classifier_executor,
                    self.generate_switch_case_edge_group(sub_workflow_settings.sub_agents[0:-1], sub_workflow_settings.sub_agents[-1], sub_workflow_settings.id)
                )
                .build()
            )

            self.agent_garden[sub_workflow_settings.id] = {
                "type": agent_garden_type,
                "name": sub_workflow_settings.id,
                "content": builder.as_agent(sub_workflow_settings.id)
            } 

    def get_information_from_start_node(self, nodes: List[AgenticNode], start_node: str) -> AgenticNode:
        selected_start_node = [ node for node in nodes  if node.id == start_node  ]
        return selected_start_node[0]
    
    def generate_unique_id(self, name: str, type: str, suffix: str ) -> str:
        return f"{name}_{type}_{suffix}"

    def build_workflow(self, workflow_structure: CompletedWorkflowInformation) -> None:
        mapper_node_executor = {}
        for node in workflow_structure.nodes:
            agentic_id = node.agentic_id
            if node.type == "agent":
                mapper_node_executor[node.id] = NamedAgentExecutor(
                    self.agent_garden[agentic_id].get("content").agent,
                    id=f"{self.agent_garden[agentic_id].get("name")}_agent_{node.id}",
                    author_name=node.id
                )
            else:
                mapper_node_executor[node.id] = NamedAgentExecutor(
                    self.agent_garden[agentic_id].get("content"),
                    id=f"{self.agent_garden[agentic_id].get("name")}_sub_workflow_{node.id}",
                    author_name=node.id
                )
        
        workflow_builder = WorkflowBuilder(start_executor=mapper_node_executor[workflow_structure.start_node])

        for edge in workflow_structure.edges:
            workflow_builder = workflow_builder.add_edge(
                mapper_node_executor[edge.source.id],
                mapper_node_executor[edge.target.id]
            )
        
        self.workflow = (
            workflow_builder
            .build()
        )
    
    def create_uri_content(self, metadata: Dict[str, str]):
        return Content.from_uri(metadata.get('uri'), media_type=metadata.get('media_type'))
    
    def prepare_content(
                    self, message: str, additional_files: Optional[List[str]] = []
                    ) -> Message:
        
        additional_content_files = [
            self.create_uri_content(get_metadata_from_uri(additional_file))
            for additional_file in additional_files
        ]

        return Message(
            role="user",
            contents=[ Content.from_text(message) ,*additional_content_files]
        ) 

    async def generate_stream_content(self, message: str, additional_files: Optional[List[str]] = []) -> AsyncIterable[WorkflowEvent]:
        content = self.prepare_content(message, additional_files)
        async for event in self.workflow.run_stream(content):
            yield event
    
    async def generate_content(self, message: str, additional_files: Optional[List[str]] = []) -> WorkflowRunResult:
        workflow_response = await self.workflow.run(message)
        return workflow_response