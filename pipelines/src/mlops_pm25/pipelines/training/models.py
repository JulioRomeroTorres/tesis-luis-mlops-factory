from pydantic import BaseModel

class XgboostHyperParameters(BaseModel):
    n_estimators: int = 1000,
    learning_rate: float = 0.01,
    max_depth: int = 1,
    subsample: float = 0.5,
    colsample_bytree: float = 0.5,
    random_state: int = 20,
    n_jobs: int =-1,
    gamma: float = 0.5,
    reg_alpha: float = 0.5,
    reg_lambda: float = 1