from fastapi import FastAPI

def create_app() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health():
        return {"ok": True}

    return app
