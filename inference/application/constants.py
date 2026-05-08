from enum import Enum

class StatusEvaluationEnum(Enum):
    START = "start"
    RUNNING = "running"
    ERROR = "error"
    SUCCESS = "sucess"

class CollectionPlaygroundEnum(Enum):
    AGENTS_INFORMATION = 'agents_information'
    TOOLS_INFORMATION = 'tools_information'
    GUARDRAILS_INFORMATION = 'guardrails_information'
    WORKFLOWS_INFORMATION = 'workflows_information'
    CROSS_DOMAIN_AGENTS = 'cross_domain_agents'
    EVALUATION_INFORMATION = 'evaluations_information'
    RAW_EVALUATION_DETAILS_INFORMATION = 'raw_evaluations_details_information'
    DATASETS_INFORMATION = 'datasets_information'
    METRICS_INFORMATION = 'metrics_evaluation'
    EVALUATION_DETAILS_INFORMATION = 'evaluations_details_information'

class ContainerStorageAccountEnum(Enum):
    FILES_CONTAINER = 'ctnreu2aiasd02'

EVALUATION_BLOB_NAME = "evaluation/execution"

VALID_EVALUATION_FILES = [ 'text/csv', 'application/x-jsonlines', 'application/jsonl', 'application/octet-stream' ]