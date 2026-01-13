from fastapi import FastAPI

def make_app() -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    def health():
        return {"ok": True}

    return app