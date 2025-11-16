from pydantic import BaseModel
from typing import Optional

class schemaFields(BaseModel):
    name: str
    type: object
    default: Optional[object] = None

class SchemaCreateDto(BaseModel):
    type: str   
    namespace: str
    name: str
    fields: list[schemaFields]= None

class JobResponse(BaseModel):
    message: str
    job_id: str = None