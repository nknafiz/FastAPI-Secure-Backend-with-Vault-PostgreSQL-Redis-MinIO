#project.runserver.py

import multiprocessing
import uvicorn

if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    uvicorn.run(
        "src.app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True  
    )
