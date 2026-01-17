from pydantic import BaseModel
from typing import Optional


class QuirkResponse(BaseModel):
    name: str
    description: str
    img: str
    

    class Config:
        from_attributes = True