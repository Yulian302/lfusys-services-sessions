from fastapi import FastAPI
from fastapi.routing import APIRouter
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

CLIENT_ORIGIN = "127.0.0.1:3000"

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[CLIENT_ORIGIN],
    allow_headers=["*"],
    allow_methods=["*"],
)


@app.get("/test")
def test():
    return JSONResponse({"message": "success"})
