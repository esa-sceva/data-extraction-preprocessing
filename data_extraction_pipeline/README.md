### How to run this pipeline

1. Start the fastapi server after changing the port number in `app.py` in one terminal.
2. Open another terminal, run the `main_multiprocess.py` file after matching the port number of the fastapi endpoint.

Ideas
1. Start a fastapi server for one folder within the same GPU. example 3 fastapi servers on 3 ports and 3 multiprocesss script for each folder.
2. Use multiple workers using uvicorn itself and run multiprocess script.
