from __future__ import annotations

import os
from functools import lru_cache
from urllib.parse import quote_plus

from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """
    Cria e cacheia a Engine do SQLAlchemy usando variáveis do .env.
    """
    load_dotenv()  # carrega .env (raiz do projeto)

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD", "")
    database = os.getenv("PGDATABASE")

    if not host or not user or not database:
        raise RuntimeError(
            "Variáveis de ambiente insuficientes. Verifique PGHOST, PGUSER e PGDATABASE no .env"
        )

    sslmode = os.getenv("PGSSLMODE")  # opcional

    pwd = quote_plus(password)
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{database}"

    connect_args = {}
    if sslmode:
        connect_args["sslmode"] = sslmode

    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        connect_args=connect_args,
    )
    return engine
