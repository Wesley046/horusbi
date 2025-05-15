from fastapi import FastAPI

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Horus BI está no ar!"}

