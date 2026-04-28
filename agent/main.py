from fastapi import FastAPI, HTTPException
from llm import get_llm_client
from prompts import build_sql_prompt
from pydantic import BaseModel

app = FastAPI()


class Payload(BaseModel):
    user_message: str


system_prompt = build_sql_prompt()
llm_client = get_llm_client()


@app.get("/")
def root():
    return {"message": "Welcome to the SD Pipe SQL Generator API. Use the /generate endpoint to generate SQL queries."}


@app.post("/generate")
def generate_sql(payload: Payload):
    if payload.user_message.strip() == "":
        raise HTTPException(status_code=400, detail="Please provide a natural language query.")
    response = llm_client.generate(system=system_prompt, user_message=payload.user_message)
    return {"sql_query": response}
