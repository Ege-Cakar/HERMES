from pydantic import BaseModel

class Pair(BaseModel):
    id: str
    nl_text: str
    lean_text: str
