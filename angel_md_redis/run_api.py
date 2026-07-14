import os

import uvicorn


def main():
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", "8000"))
    uvicorn.run("app.api:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
