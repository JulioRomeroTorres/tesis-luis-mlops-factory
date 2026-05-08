from typing import List, Any, Dict
from app.application.services.agent_information_manager import AgentInformationManager
from app.domain.utils import generate_uuid, get_datetime_now, grouped_by_key
from app.domain.agent.agent import (
    SimplifyAgentInformation,
    CompletedAgentInformation,
    CrossDomainAgentInformation
) 

class HandleAgentsUseCase:
    
    def __init__(self, agent_information_manager: AgentInformationManager):
        self.agent_information_manager = agent_information_manager
        pass
    
    def format_groupped_agents(self, groupped_agents: Dict[str, List[Any]]) -> Dict[str, Any]:
        formatted_information = []

        for agent_name in groupped_agents.keys():
            formatted_information.append(
                {
                    "name": agent_name,
                    "versions": groupped_agents[agent_name],
                    "latest_version": groupped_agents[agent_name][-1]["version"]
                }
            )
        return formatted_information

    async def create_agent(self, user_id: str, agent_information: Any) -> SimplifyAgentInformation:
        all_agent_information = {
            "created_by": user_id, 
            "agent_id": f"{generate_uuid()}", 
            "created_at": get_datetime_now(),
            **agent_information.model_dump()
            }
        print("ajajaja", all_agent_information, agent_information.model_dump())
        created_register = await self.agent_information_manager.create_agent(all_agent_information)
        
        if len(created_register) < 1:
            print("Error al encontrar el agente")

        print("register", created_register[0])

        return SimplifyAgentInformation(**created_register[0])
        
    async def get_agents_by_user(self, user_id: str) -> List[SimplifyAgentInformation]:
        all_agents = await self.agent_information_manager.get_agents_by_user(user_id)
        all_agents = [ SimplifyAgentInformation(**agent).format_json()  for agent in all_agents  ]
        groupped_agents = grouped_by_key(all_agents, "name", "created_at")    
        return self.format_groupped_agents(groupped_agents)      

    async def get_agent_by_user(self, agent_id: str) -> CompletedAgentInformation:
        selected_agent = await self.agent_information_manager.get_specific_agent_by_user(agent_id)
        return CompletedAgentInformation(**selected_agent)
    
    async def get_agent_version(self, agent_name: str) -> List[SimplifyAgentInformation]:
        selected_agents = await self.agent_information_manager.get_agent_versions(agent_name)
        return [ SimplifyAgentInformation(**agent)  for agent in selected_agents  ]      

    async def get_cross_domain_agents(self) -> List[CrossDomainAgentInformation]:
        selected_agents = await self.agent_information_manager.get_cross_domain_agents()
        print("Mi cross agents", selected_agents)
        return [ CrossDomainAgentInformation(**agent)  for agent in selected_agents  ]            

         